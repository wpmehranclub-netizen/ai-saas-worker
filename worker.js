/**
 * AI SaaS Queue Worker
 * File: worker.js
 * Deploy on Railway.app
 */

import { Worker, Queue } from 'bullmq';
import mysql from 'mysql2/promise';
import axios from 'axios';
import FormData from 'form-data';
import * as dotenv from 'dotenv';

dotenv.config();

// ─────────────────────────────────────────────────────────────
// REDIS CONFIG — Redis Cloud with correct TLS
// ─────────────────────────────────────────────────────────────
const REDIS_CONFIG = {
    host:     process.env.REDIS_HOST,
    port:     parseInt(process.env.REDIS_PORT || '13906'),
    username: process.env.REDIS_USER || 'default',
    password: process.env.REDIS_PASS,
    tls:      {
        rejectUnauthorized: false,
    },
    maxRetriesPerRequest: null,
    enableReadyCheck:     false,
};

// ─────────────────────────────────────────────────────────────
// DB CONFIG
// ─────────────────────────────────────────────────────────────
const DB_CONFIG = {
    host:               process.env.DB_HOST,
    port:               parseInt(process.env.DB_PORT || '3306'),
    user:               process.env.DB_USER,
    password:           process.env.DB_PASS,
    database:           process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit:    20,
    queueLimit:         0,
    connectTimeout:     30000,
};

const TABLE_PREFIX      = process.env.WP_TABLE_PREFIX || 'wp_';
const TABLE_QUEUE       = `${TABLE_PREFIX}ais_queue`;
const TABLE_GENERATIONS = `${TABLE_PREFIX}ais_generations`;
const TABLE_CREDITS     = `${TABLE_PREFIX}ais_credits`;

// ─────────────────────────────────────────────────────────────
// AI33 ACCOUNT ROTATION
// ─────────────────────────────────────────────────────────────
const AI33_AUDIO_ACCOUNTS = process.env.AI33_AUDIO_KEYS
    ? process.env.AI33_AUDIO_KEYS.split(',').map(k => k.trim()).filter(Boolean)
    : [process.env.AI33_AUDIO_KEY].filter(Boolean);

const AI33_BASE_URL = 'https://api.ai33.pro';
const WEBHOOK_URL   = process.env.WEBHOOK_URL;

const accountTracker = AI33_AUDIO_ACCOUNTS.map(key => ({
    key,
    active: 0,
    limit:  20,
}));

function getAvailableAccount() {
    return accountTracker.find(acc => acc.active < acc.limit) || accountTracker[0];
}

function incrementAccount(key) {
    const acc = accountTracker.find(a => a.key === key);
    if (acc) acc.active = Math.min(acc.active + 1, acc.limit);
}

function decrementAccount(key) {
    const acc = accountTracker.find(a => a.key === key);
    if (acc && acc.active > 0) acc.active--;
}

// ─────────────────────────────────────────────────────────────
// MYSQL POOL
// ─────────────────────────────────────────────────────────────
let db;

async function getDB() {
    if (!db) {
        db = await mysql.createPool(DB_CONFIG);
    }
    return db;
}

async function dbQuery(sql, params = []) {
    const pool = await getDB();
    const [rows] = await pool.execute(sql, params);
    return rows;
}

async function updateQueueRow(predictionId, fields) {
    const sets   = Object.keys(fields).map(k => `${k} = ?`).join(', ');
    const values = [...Object.values(fields), predictionId];
    await dbQuery(
        `UPDATE ${TABLE_QUEUE} SET ${sets}, updated_at = NOW() WHERE prediction_id = ?`,
        values
    );
}

async function updateGenerationsRow(predictionId, fields) {
    const sets   = Object.keys(fields).map(k => `${k} = ?`).join(', ');
    const values = [...Object.values(fields), predictionId];
    await dbQuery(
        `UPDATE ${TABLE_GENERATIONS} SET ${sets}, updated_at = NOW() WHERE prediction_id = ?`,
        values
    );
}

async function refundCredits(predictionId, reason) {
    try {
        const rows = await dbQuery(
            `SELECT user_id, credit_cost FROM ${TABLE_GENERATIONS} WHERE prediction_id = ? LIMIT 1`,
            [predictionId]
        );
        if (!rows.length || !rows[0].credit_cost) return;
        const { user_id, credit_cost } = rows[0];
        await dbQuery(
            `INSERT INTO ${TABLE_CREDITS} (user_id, amount, type, note, created_at) VALUES (?, ?, 'refund', ?, NOW())`,
            [user_id, credit_cost, reason]
        );
        console.log(`[Credits] Refunded ${credit_cost} to user ${user_id}`);
    } catch (err) {
        console.error('[Credits] Refund error:', err.message);
    }
}

// ─────────────────────────────────────────────────────────────
// AI33 REQUEST
// ─────────────────────────────────────────────────────────────
async function ai33Request(endpoint, method = 'POST', data = null, apiKey, isFormData = false) {
    const url     = `${AI33_BASE_URL}${endpoint}`;
    const headers = { 'xi-api-key': apiKey };

    if (isFormData && data instanceof FormData) {
        Object.assign(headers, data.getHeaders());
    } else if (data) {
        headers['Content-Type'] = 'application/json';
    }

    const response = await axios({
        method,
        url,
        headers,
        data:    isFormData ? data : (data ? JSON.stringify(data) : undefined),
        timeout: 60000,
    });

    return response.data;
}

// ─────────────────────────────────────────────────────────────
// JOB PROCESSORS
// ─────────────────────────────────────────────────────────────
async function processTTS(job) {
    const {
        prediction_id, voice_id, text, model_id,
        output_format, voice_settings
    } = job.data;

    const account = getAvailableAccount();
    incrementAccount(account.key);

    try {
        console.log(`[TTS] Processing ${prediction_id}...`);
        await updateQueueRow(prediction_id, { status: 'processing' });
        await updateGenerationsRow(prediction_id, { status: 'processing' });

        const res = await ai33Request(
            `/v1/text-to-speech/${voice_id}?output_format=${output_format || 'mp3_44100_128'}`,
            'POST',
            {
                text,
                model_id,
                with_transcript:          false,
                voice_settings:           voice_settings || {},
                apply_text_normalization: 'auto',
                receive_url:              WEBHOOK_URL,
            },
            account.key
        );

        if (!res.success) throw new Error(res.message || res.detail || 'TTS failed');

        const taskId = res.task_id || res.data?.task_id;
        if (!taskId) throw new Error('No task_id returned from AI33');

        await dbQuery(
            `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );
        await dbQuery(
            `UPDATE ${TABLE_GENERATIONS} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );

        console.log(`[TTS] ✅ Submitted. AI33 task_id: ${taskId}`);

    } catch (err) {
        console.error(`[TTS] ❌ Error: ${err.message}`);
        await updateQueueRow(prediction_id, { status: 'failed', error_message: err.message });
        await updateGenerationsRow(prediction_id, { status: 'failed' });
        await refundCredits(prediction_id, `tts_failed: ${err.message}`);
        throw err;
    } finally {
        decrementAccount(account.key);
    }
}

async function processChangeVoice(job) {
    const { prediction_id, voice_id, model_id, audio_url, voice_settings } = job.data;
    const account = getAvailableAccount();
    incrementAccount(account.key);

    try {
        console.log(`[ChangeVoice] Processing ${prediction_id}...`);
        await updateQueueRow(prediction_id, { status: 'processing' });

        const fd        = new FormData();
        const audioResp = await axios.get(audio_url, { responseType: 'arraybuffer', timeout: 30000 });
        fd.append('file', Buffer.from(audioResp.data), { filename: 'audio.mp3', contentType: 'audio/mpeg' });
        fd.append('voice_id', voice_id);
        fd.append('model_id', model_id || 'eleven_multilingual_sts_v2');
        fd.append('voice_settings', JSON.stringify(voice_settings || {}));
        fd.append('remove_background_noise', 'false');
        if (WEBHOOK_URL) fd.append('receive_url', WEBHOOK_URL);

        const res = await ai33Request('/v1/task/voice-changer', 'POST', fd, account.key, true);
        if (!res.success) throw new Error(res.message || 'Change voice failed');

        const taskId = res.task_id || res.data?.task_id;
        await dbQuery(
            `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );

        console.log(`[ChangeVoice] ✅ Submitted. task_id: ${taskId}`);

    } catch (err) {
        console.error(`[ChangeVoice] ❌ Error: ${err.message}`);
        await updateQueueRow(prediction_id, { status: 'failed', error_message: err.message });
        throw err;
    } finally {
        decrementAccount(account.key);
    }
}

async function processDub(job) {
    const { prediction_id, audio_url, target_lang, source_lang } = job.data;
    const account = getAvailableAccount();
    incrementAccount(account.key);

    try {
        console.log(`[Dub] Processing ${prediction_id}...`);
        await updateQueueRow(prediction_id, { status: 'processing' });

        const fd        = new FormData();
        const audioResp = await axios.get(audio_url, { responseType: 'arraybuffer', timeout: 30000 });
        fd.append('file', Buffer.from(audioResp.data), { filename: 'audio.mp3', contentType: 'audio/mpeg' });
        fd.append('target_lang', target_lang);
        fd.append('source_lang', source_lang || 'auto');
        fd.append('num_speakers', '0');
        fd.append('disable_voice_cloning', 'false');
        if (WEBHOOK_URL) fd.append('receive_url', WEBHOOK_URL);

        const res = await ai33Request('/v1/task/dubbing', 'POST', fd, account.key, true);
        if (!res.success) throw new Error(res.message || 'Dub failed');

        const taskId = res.task_id || res.data?.task_id;
        await dbQuery(
            `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );

        console.log(`[Dub] ✅ Submitted. task_id: ${taskId}`);

    } catch (err) {
        console.error(`[Dub] ❌ Error: ${err.message}`);
        await updateQueueRow(prediction_id, { status: 'failed', error_message: err.message });
        throw err;
    } finally {
        decrementAccount(account.key);
    }
}

// ─────────────────────────────────────────────────────────────
// JOB DISPATCHER
// ─────────────────────────────────────────────────────────────
async function processJob(job) {
    console.log(`[Worker] Job: ${job.name} | id: ${job.id}`);
    switch (job.name) {
        case 'audio_tts':          return await processTTS(job);
        case 'audio_change_voice': return await processChangeVoice(job);
        case 'audio_dub':          return await processDub(job);
        default:
            console.warn(`[Worker] Unknown job type: ${job.name}`);
    }
}

// ─────────────────────────────────────────────────────────────
// BULL BOARD DASHBOARD
// ─────────────────────────────────────────────────────────────
async function startDashboard(queue) {
    if (process.env.ENABLE_DASHBOARD !== 'true') return;

    const { createBullBoard } = await import('@bull-board/api');
    const { BullMQAdapter }   = await import('@bull-board/api/bullMQAdapter.js');
    const { ExpressAdapter }  = await import('@bull-board/express');
    const express             = (await import('express')).default;

    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath('/dashboard');

    createBullBoard({ queues: [new BullMQAdapter(queue)], serverAdapter });

    const app  = express();
    const port = process.env.PORT || 3000;

    // Basic auth
    app.use('/dashboard', (req, res, next) => {
        const b64      = (req.headers.authorization || '').split(' ')[1] || '';
        const [l, p]   = Buffer.from(b64, 'base64').toString().split(':');
        const authUser = process.env.DASHBOARD_USER || 'admin';
        const authPass = process.env.DASHBOARD_PASS || 'admin';
        if (l === authUser && p === authPass) return next();
        res.set('WWW-Authenticate', 'Basic realm="AI SaaS Jobs"');
        res.status(401).send('Auth required');
    });

    app.use('/dashboard', serverAdapter.getRouter());

    app.get('/health', (req, res) => {
        res.json({
            status:   'ok',
            accounts: accountTracker.map(a => ({
                key:    a.key.substring(0, 8) + '...',
                active: a.active,
                limit:  a.limit,
            })),
        });
    });

    app.listen(port, () => {
        console.log(`[Dashboard] Running at http://localhost:${port}/dashboard`);
        console.log(`[Health]    Running at http://localhost:${port}/health`);
    });
}

// ─────────────────────────────────────────────────────────────
// START
// ─────────────────────────────────────────────────────────────
async function start() {
    console.log('[Worker] Starting AI SaaS Queue Worker...');
    console.log(`[Worker] Redis: ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`);
    console.log(`[Worker] AI33 accounts: ${AI33_AUDIO_ACCOUNTS.length}`);
    console.log(`[Worker] Concurrency: ${process.env.WORKER_CONCURRENCY || 20}`);

    // Test DB
    try {
        await dbQuery('SELECT 1');
        console.log('[Worker] MySQL connected ✅');
    } catch (err) {
        console.error('[Worker] MySQL connection failed:', err.message);
        process.exit(1);
    }

    const concurrency = parseInt(process.env.WORKER_CONCURRENCY || '20');

    const worker = new Worker('ai-saas-jobs', processJob, {
        connection:  REDIS_CONFIG,
        concurrency: concurrency,
    });

    worker.on('completed', job => {
        console.log(`[Worker] ✅ Done: ${job.name} | ${job.id}`);
    });

    worker.on('failed', (job, err) => {
        console.error(`[Worker] ❌ Failed: ${job?.name} | ${err.message}`);
    });

    worker.on('error', err => {
        console.error('[Worker] Worker error:', err.message);
    });

    const queue = new Queue('ai-saas-jobs', { connection: REDIS_CONFIG });
    await startDashboard(queue);

    process.on('SIGTERM', async () => {
        console.log('[Worker] Shutting down...');
        await worker.close();
        await queue.close();
        process.exit(0);
    });

    process.on('SIGINT', async () => {
        await worker.close();
        process.exit(0);
    });

    console.log('[Worker] ✅ Ready. Waiting for jobs...');
}

start().catch(err => {
    console.error('[Worker] Fatal:', err.message);
    process.exit(1);
});
