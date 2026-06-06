/**
 * AI SaaS Queue Worker
 * File: worker.js
 * 
 * Deploy this on Railway.app
 * Reads jobs from Redis Cloud → processes → updates MySQL
 * 
 * Supports: audio TTS, change-voice, dub, music (future)
 */

import { Worker, Queue, QueueEvents } from 'bullmq';
import mysql from 'mysql2/promise';
import axios from 'axios';
import FormData from 'form-data';
import * as dotenv from 'dotenv';

dotenv.config();

// ─────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────
const REDIS_CONFIG = process.env.REDIS_TLS === 'true'
    ? `rediss://${process.env.REDIS_USER}:${process.env.REDIS_PASS}@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`
    : `redis://${process.env.REDIS_USER}:${process.env.REDIS_PASS}@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`;

const DB_CONFIG = {
    host:     process.env.DB_HOST,
    port:     parseInt(process.env.DB_PORT || '3306'),
    user:     process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit:    20,
    queueLimit:         0,
};

const TABLE_PREFIX      = process.env.WP_TABLE_PREFIX || 'wp_';
const TABLE_QUEUE       = `${TABLE_PREFIX}ais_queue`;
const TABLE_GENERATIONS = `${TABLE_PREFIX}ais_generations`;
const TABLE_CREDITS     = `${TABLE_PREFIX}ais_credits`;

// AI33 audio accounts — rotate across them for 200 concurrent
const AI33_AUDIO_ACCOUNTS = process.env.AI33_AUDIO_KEYS
    ? process.env.AI33_AUDIO_KEYS.split(',').map(k => k.trim())
    : [process.env.AI33_AUDIO_KEY];

const AI33_BASE_URL   = 'https://api.ai33.pro';
const WEBHOOK_URL     = process.env.WEBHOOK_URL; // https://beducate.site/wp-json/ai-saas/v1/audio/webhook

// ─────────────────────────────────────────────────────────────
// ACCOUNT ROTATION
// Track active jobs per account to respect 20-concurrent limit
// ─────────────────────────────────────────────────────────────
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
    if (acc) acc.active++;
}

function decrementAccount(key) {
    const acc = accountTracker.find(a => a.key === key);
    if (acc && acc.active > 0) acc.active--;
}

// ─────────────────────────────────────────────────────────────
// MYSQL POOL
// ─────────────────────────────────────────────────────────────
const db = mysql.createPool(DB_CONFIG);

async function dbQuery(sql, params = []) {
    const [rows] = await db.execute(sql, params);
    return rows;
}

// ─────────────────────────────────────────────────────────────
// UPDATE QUEUE ROW
// ─────────────────────────────────────────────────────────────
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

// ─────────────────────────────────────────────────────────────
// REFUND CREDITS
// ─────────────────────────────────────────────────────────────
async function refundCredits(predictionId, reason) {
    const rows = await dbQuery(
        `SELECT user_id, credit_cost FROM ${TABLE_GENERATIONS} WHERE prediction_id = ? LIMIT 1`,
        [predictionId]
    );
    if (!rows.length || !rows[0].credit_cost) return;

    const { user_id, credit_cost } = rows[0];
    await dbQuery(
        `INSERT INTO ${TABLE_CREDITS} (user_id, amount, type, note, created_at)
         VALUES (?, ?, 'refund', ?, NOW())`,
        [user_id, credit_cost, reason]
    );
    console.log(`[Credits] Refunded ${credit_cost} credits to user ${user_id}: ${reason}`);
}

// ─────────────────────────────────────────────────────────────
// UPLOAD CDN URL → R2 via WordPress webhook
// WordPress handles R2 upload — worker just notifies via webhook
// ─────────────────────────────────────────────────────────────
async function notifyWebhook(payload) {
    if (!WEBHOOK_URL) return payload.output_uri || '';
    
    try {
        await axios.post(WEBHOOK_URL, payload, {
            headers: { 'Content-Type': 'application/json' },
            timeout: 30000,
        });
        console.log(`[Webhook] Notified: ${payload.id}`);
    } catch (err) {
        console.error(`[Webhook] Failed: ${err.message}`);
    }
}

// ─────────────────────────────────────────────────────────────
// AI33 HTTP REQUEST
// ─────────────────────────────────────────────────────────────
async function ai33Request(endpoint, method = 'GET', data = null, apiKey, isFormData = false) {
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
        data: isFormData ? data : (data ? JSON.stringify(data) : undefined),
        timeout: 60000,
    });

    return response.data;
}

// ─────────────────────────────────────────────────────────────
// JOB PROCESSORS
// ─────────────────────────────────────────────────────────────

/**
 * Process TTS job
 */
async function processTTS(job) {
    const { prediction_id, voice_id, text, model_id, output_format, voice_settings } = job.data;
    const account = getAvailableAccount();
    incrementAccount(account.key);

    try {
        console.log(`[TTS] Processing ${prediction_id} via account ${account.key.substring(0, 8)}...`);

        // Update status to processing
        await updateQueueRow(prediction_id, { status: 'processing' });
        await updateGenerationsRow(prediction_id, { status: 'processing' });

        // Call AI33
        const res = await ai33Request(
            `/v1/text-to-speech/${voice_id}?output_format=${output_format || 'mp3_44100_128'}`,
            'POST',
            {
                text,
                model_id,
                with_transcript:          false,
                voice_settings,
                apply_text_normalization: 'auto',
                receive_url:              WEBHOOK_URL,
            },
            account.key
        );

        if (!res.success) {
            throw new Error(res.message || res.detail || 'TTS failed');
        }

        const taskId = res.task_id || res.data?.task_id;

        // Update prediction_id with real AI33 task_id
        await dbQuery(
            `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );
        await dbQuery(
            `UPDATE ${TABLE_GENERATIONS} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );

        console.log(`[TTS] Submitted to AI33. task_id: ${taskId}`);
        // Webhook will fire when done — no polling needed

    } catch (err) {
        console.error(`[TTS] Error ${prediction_id}: ${err.message}`);
        await updateQueueRow(prediction_id, { status: 'failed', error_message: err.message });
        await updateGenerationsRow(prediction_id, { status: 'failed' });
        await refundCredits(prediction_id, `tts_failed:${err.message}`);
        throw err; // BullMQ will retry based on job options
    } finally {
        decrementAccount(account.key);
    }
}

/**
 * Process Change Voice job
 */
async function processChangeVoice(job) {
    const { prediction_id, voice_id, model_id, audio_url, voice_settings } = job.data;
    const account = getAvailableAccount();
    incrementAccount(account.key);

    try {
        console.log(`[ChangeVoice] Processing ${prediction_id}...`);
        await updateQueueRow(prediction_id, { status: 'processing' });

        const fd = new FormData();
        // Download audio from temp URL and attach
        const audioResp = await axios.get(audio_url, { responseType: 'arraybuffer' });
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

        console.log(`[ChangeVoice] Submitted. task_id: ${taskId}`);

    } catch (err) {
        console.error(`[ChangeVoice] Error: ${err.message}`);
        await updateQueueRow(prediction_id, { status: 'failed', error_message: err.message });
        throw err;
    } finally {
        decrementAccount(account.key);
    }
}

/**
 * Process Dub job
 */
async function processDub(job) {
    const { prediction_id, audio_url, target_lang, source_lang } = job.data;
    const account = getAvailableAccount();
    incrementAccount(account.key);

    try {
        console.log(`[Dub] Processing ${prediction_id}...`);
        await updateQueueRow(prediction_id, { status: 'processing' });

        const fd = new FormData();
        const audioResp = await axios.get(audio_url, { responseType: 'arraybuffer' });
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

        console.log(`[Dub] Submitted. task_id: ${taskId}`);

    } catch (err) {
        console.error(`[Dub] Error: ${err.message}`);
        await updateQueueRow(prediction_id, { status: 'failed', error_message: err.message });
        throw err;
    } finally {
        decrementAccount(account.key);
    }
}

// ─────────────────────────────────────────────────────────────
// MAIN JOB DISPATCHER
// Routes jobs to correct processor based on job.name
// ─────────────────────────────────────────────────────────────
async function processJob(job) {
    console.log(`[Worker] Job received: ${job.name} | id: ${job.id}`);

    switch (job.name) {
        case 'audio_tts':
            return await processTTS(job);
        case 'audio_change_voice':
            return await processChangeVoice(job);
        case 'audio_dub':
            return await processDub(job);
        // Future: music, image, video
        // case 'music_generate':
        //     return await processMusic(job);
        default:
            console.warn(`[Worker] Unknown job type: ${job.name}`);
    }
}

// ─────────────────────────────────────────────────────────────
// BULL BOARD DASHBOARD (optional monitoring UI)
// ─────────────────────────────────────────────────────────────
async function startDashboard(queue) {
    if (process.env.ENABLE_DASHBOARD !== 'true') return;

    const { createBullBoard } = await import('@bull-board/api');
    const { BullMQAdapter }   = await import('@bull-board/api/bullMQAdapter.js');
    const { ExpressAdapter }  = await import('@bull-board/express');
    const express             = (await import('express')).default;

    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath('/dashboard');

    createBullBoard({
        queues:        [new BullMQAdapter(queue)],
        serverAdapter: serverAdapter,
    });

    const app  = express();
    const port = process.env.PORT || 3000;

    // Basic auth for dashboard security
    app.use('/dashboard', (req, res, next) => {
        const auth = { login: process.env.DASHBOARD_USER || 'admin', password: process.env.DASHBOARD_PASS || 'admin' };
        const b64  = (req.headers.authorization || '').split(' ')[1] || '';
        const [login, password] = Buffer.from(b64, 'base64').toString().split(':');
        if (login === auth.login && password === auth.password) return next();
        res.set('WWW-Authenticate', 'Basic realm="AI SaaS Jobs"');
        res.status(401).send('Authentication required');
    });

    app.use('/dashboard', serverAdapter.getRouter());

    // Health check endpoint
    app.get('/health', (req, res) => {
        res.json({
            status:   'ok',
            accounts: accountTracker.map(a => ({
                key_prefix: a.key.substring(0, 8) + '...',
                active:     a.active,
                limit:      a.limit,
            })),
        });
    });

    app.listen(port, () => {
        console.log(`[Dashboard] Running at http://localhost:${port}/dashboard`);
        console.log(`[Health]    Running at http://localhost:${port}/health`);
    });
}

// ─────────────────────────────────────────────────────────────
// START WORKER
// ─────────────────────────────────────────────────────────────
async function start() {
    console.log('[Worker] Starting AI SaaS Queue Worker...');
    console.log(`[Worker] Redis: ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`);
    console.log(`[Worker] AI33 accounts: ${AI33_AUDIO_ACCOUNTS.length}`);
    console.log(`[Worker] Concurrency: ${process.env.WORKER_CONCURRENCY || 20} per worker`);

    // Test DB connection
    try {
        await dbQuery('SELECT 1');
        console.log('[Worker] MySQL connected ✅');
    } catch (err) {
        console.error('[Worker] MySQL connection failed:', err.message);
        process.exit(1);
    }

    const concurrency = parseInt(process.env.WORKER_CONCURRENCY || '20');

    // Create BullMQ worker
    const worker = new Worker(
        'ai-saas-jobs',
        processJob,
        {
            connection:  REDIS_CONFIG,
            concurrency: concurrency,
            // Retry failed jobs 3 times with exponential backoff
            settings: {
                backoffStrategy: (attemptsMade) => Math.pow(2, attemptsMade) * 1000,
            },
        }
    );

    // Worker events
    worker.on('completed', (job) => {
        console.log(`[Worker] ✅ Job completed: ${job.name} | ${job.id}`);
    });

    worker.on('failed', (job, err) => {
        console.error(`[Worker] ❌ Job failed: ${job?.name} | ${job?.id} | ${err.message}`);
    });

    worker.on('error', (err) => {
        console.error('[Worker] Worker error:', err.message);
    });

    worker.on('stalled', (jobId) => {
        console.warn(`[Worker] ⚠️ Job stalled: ${jobId}`);
    });

    // Create queue reference for dashboard
    const queue = new Queue('ai-saas-jobs', { connection: REDIS_CONFIG });

    // Start dashboard if enabled
    await startDashboard(queue);

    // Graceful shutdown
    process.on('SIGTERM', async () => {
        console.log('[Worker] SIGTERM received — shutting down gracefully...');
        await worker.close();
        await queue.close();
        await db.end();
        process.exit(0);
    });

    process.on('SIGINT', async () => {
        console.log('[Worker] SIGINT received — shutting down...');
        await worker.close();
        process.exit(0);
    });

    console.log('[Worker] ✅ Ready. Waiting for jobs...');
}

start().catch(err => {
    console.error('[Worker] Fatal error:', err);
    process.exit(1);
});
