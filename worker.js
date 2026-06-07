/**
 * AI SaaS Queue Worker
 * File: worker.js
 *
 * Phase 2 additions:
 * - decrementCounters() — decrements user + platform + pool counters
 * - KIE rate limit changed to 18 (safe buffer from 20 limit)
 * - All processors use decrementCounters() instead of decrementUserCounter()
 */

import { Worker, Queue } from 'bullmq';
import mysql              from 'mysql2/promise';
import axios              from 'axios';
import FormData           from 'form-data';
import * as dotenv        from 'dotenv';

dotenv.config();

// ─────────────────────────────────────────────────────────────
// REDIS CONFIG
// ─────────────────────────────────────────────────────────────
const REDIS_CONFIG = {
    host:                 process.env.REDIS_HOST,
    port:                 parseInt( process.env.REDIS_PORT || '13906' ),
    username:             process.env.REDIS_USER || 'default',
    password:             process.env.REDIS_PASS,
    maxRetriesPerRequest: null,
    enableReadyCheck:     false,
};

// ─────────────────────────────────────────────────────────────
// ACCOUNT POOLS
// ─────────────────────────────────────────────────────────────
const POOLS = {

    ai33_audio: {
        type:      'slot',
        limit:     parseInt( process.env.AI33_SLOT_LIMIT || '15' ),
        accounts:  parseAccounts( process.env.AI33_AUDIO_KEYS || process.env.AI33_AUDIO_KEY || '' ),
        baseUrl:   'https://api.ai33.pro',
        authStyle: 'xi-api-key',
        jobTypes:  [ 'audio_tts', 'audio_change_voice', 'audio_dub' ],
    },

    ai33_image: {
        type:      'slot',
        limit:     parseInt( process.env.AI33_SLOT_LIMIT || '15' ),
        accounts:  parseAccounts( process.env.AI33_IMAGE_KEYS || process.env.AI33_IMAGE_KEY || '' ),
        baseUrl:   'https://api.ai33.pro',
        authStyle: 'xi-api-key',
        jobTypes:  [ 'image_generate' ],
    },

    ai33_video: {
        type:      'slot',
        limit:     parseInt( process.env.AI33_SLOT_LIMIT || '15' ),
        accounts:  parseAccounts( process.env.AI33_VIDEO_KEYS || process.env.AI33_VIDEO_KEY || '' ),
        baseUrl:   'https://api.ai33.pro',
        authStyle: 'xi-api-key',
        jobTypes:  [ 'video_generate' ],
    },

    // KIE — 18 per 10s (safe buffer below KIE's hard limit of 20)
    kie: {
        type:      'rate',
        limit:     parseInt( process.env.KIE_RATE_LIMIT || '18' ),
        windowMs:  parseInt( process.env.KIE_WINDOW_MS  || '10000' ),
        accounts:  parseAccounts( process.env.KIE_KEYS  || process.env.KIE_KEY || '' ),
        baseUrl:   'https://api.kie.ai',
        authStyle: 'bearer',
        jobTypes:  [ 'image_generate', 'video_generate', 'music_generate' ],
    },

    // Future providers — add here:
    // replicate: { type: 'slot', limit: 10, accounts: parseAccounts(process.env.REPLICATE_KEYS), ... }
};

// Job type → pool name mapping — must match PHP JOB_POOL_MAP
const JOB_POOL_MAP = {
    audio_tts:          'audio',
    audio_change_voice: 'audio',
    audio_dub:          'audio',
    image_generate:     'image',
    video_generate:     'video',
    music_generate:     'music',
};

// ─────────────────────────────────────────────────────────────
// DB CONFIG
// ─────────────────────────────────────────────────────────────
const DB_CONFIG = {
    host:               process.env.DB_HOST,
    port:               parseInt( process.env.DB_PORT || '3306' ),
    user:               process.env.DB_USER,
    password:           process.env.DB_PASS,
    database:           process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit:    20,
    connectTimeout:     30000,
};

const TABLE_PREFIX      = process.env.WP_TABLE_PREFIX || 'wp_';
const TABLE_QUEUE       = `${TABLE_PREFIX}ais_queue`;
const TABLE_GENERATIONS = `${TABLE_PREFIX}ais_generations`;
const TABLE_CREDITS     = `${TABLE_PREFIX}ais_credits`;

const WEBHOOK_URL = process.env.WEBHOOK_URL;

// ─────────────────────────────────────────────────────────────
// PARSE ACCOUNTS
// ─────────────────────────────────────────────────────────────
function parseAccounts( input ) {
    if ( ! input ) return [];
    return input.split( ',' )
        .map( ( k, i ) => ({ id: `account_${i + 1}`, key: k.trim(), enabled: true }) )
        .filter( a => a.key.length > 0 );
}

// ─────────────────────────────────────────────────────────────
// REDIS CLIENT
// ─────────────────────────────────────────────────────────────
let redisClient = null;

async function getRedis() {
    if ( redisClient ) return redisClient;
    const { Redis } = await import( 'ioredis' );
    redisClient = new Redis( REDIS_CONFIG );
    return redisClient;
}

// ─────────────────────────────────────────────────────────────
// ACCOUNT POOL — get available account
// ─────────────────────────────────────────────────────────────
async function getAvailableAccount( poolName ) {
    const pool = POOLS[ poolName ];
    if ( ! pool || ! pool.accounts.length ) return null;

    const redis = await getRedis();

    for ( const account of pool.accounts ) {
        if ( ! account.enabled ) continue;

        const accountId = account.id;
        const disabled  = await redis.exists( `pool:disabled:${poolName}:${accountId}` );
        if ( disabled ) continue;

        if ( pool.type === 'slot' ) {
            const key    = `pool:slots:${poolName}:${accountId}`;
            const newVal = await redis.incr( key );
            await redis.expire( key, 3600 );

            if ( newVal <= pool.limit ) {
                return { ...account, pool: poolName };
            }
            await redis.decr( key );

        } else if ( pool.type === 'rate' ) {
            const key    = `pool:rate:${poolName}:${accountId}`;
            const now    = Date.now();
            const window = pool.windowMs || 10000;

            await redis.zremrangebyscore( key, 0, now - window );
            const count = await redis.zcard( key );

            if ( count < pool.limit ) {
                await redis.zadd( key, now, `${now}-${Math.random()}` );
                await redis.expire( key, Math.ceil( window / 1000 ) + 5 );
                return { ...account, pool: poolName };
            }
        }
    }

    return null;
}

// ─────────────────────────────────────────────────────────────
// RELEASE SLOT
// ─────────────────────────────────────────────────────────────
async function releaseSlot( poolName, accountId ) {
    const pool = POOLS[ poolName ];
    if ( ! pool || pool.type !== 'slot' ) return;

    const redis = await getRedis();
    const key   = `pool:slots:${poolName}:${accountId}`;
    const val   = parseInt( await redis.get( key ) ) || 0;
    if ( val > 0 ) await redis.decr( key );
}

// ─────────────────────────────────────────────────────────────
// DISABLE ACCOUNT
// ─────────────────────────────────────────────────────────────
async function disableAccount( poolName, accountId, seconds = 3600 ) {
    const redis = await getRedis();
    await redis.setex( `pool:disabled:${poolName}:${accountId}`, seconds, 1 );
    console.warn( `[Pool] ⚠️ Disabled: ${poolName}:${accountId} for ${seconds}s` );
}

// ─────────────────────────────────────────────────────────────
// DECREMENT ALL COUNTERS — user + platform + pool
// Call on job complete or fail
// ─────────────────────────────────────────────────────────────
async function decrementCounters( userId, jobType ) {
    try {
        const redis    = await getRedis();
        const poolName = JOB_POOL_MAP[ jobType ] || 'general';

        // User counter
        const userKey = `ai_saas:concurrent:${userId}`;
        const userVal = parseInt( await redis.get( userKey ) ) || 0;
        if ( userVal > 0 ) await redis.decr( userKey );

        // Platform counter
        const platKey = 'ai_saas:platform:total_active';
        const platVal = parseInt( await redis.get( platKey ) ) || 0;
        if ( platVal > 0 ) await redis.decr( platKey );

        // Pool counter
        const poolKey = `ai_saas:pool:${poolName}:active`;
        const poolVal = parseInt( await redis.get( poolKey ) ) || 0;
        if ( poolVal > 0 ) await redis.decr( poolKey );

    } catch ( err ) {
        console.error( '[Counter] Decrement error:', err.message );
    }
}

// ─────────────────────────────────────────────────────────────
// MYSQL POOL
// ─────────────────────────────────────────────────────────────
let db;

async function getDB() {
    if ( ! db ) db = await mysql.createPool( DB_CONFIG );
    return db;
}

async function dbQuery( sql, params = [] ) {
    const pool   = await getDB();
    const [rows] = await pool.execute( sql, params );
    return rows;
}

// ─────────────────────────────────────────────────────────────
// DB HELPERS
// ─────────────────────────────────────────────────────────────
async function updateQueueRow( predictionId, fields ) {
    const sets   = Object.keys( fields ).map( k => `${k} = ?` ).join( ', ' );
    const values = [ ...Object.values( fields ), predictionId ];
    await dbQuery(
        `UPDATE ${TABLE_QUEUE} SET ${sets}, updated_at = NOW() WHERE prediction_id = ?`,
        values
    );
}

async function refundCredits( predictionId, reason ) {
    try {
        const rows = await dbQuery(
            `SELECT user_id, credit_cost FROM ${TABLE_GENERATIONS} WHERE prediction_id = ? LIMIT 1`,
            [predictionId]
        );
        if ( ! rows.length || ! rows[0].credit_cost ) return;
        const { user_id, credit_cost } = rows[0];
        await dbQuery(
            `INSERT INTO ${TABLE_CREDITS} (user_id, amount, type, note, created_at) VALUES (?, ?, 'refund', ?, NOW())`,
            [user_id, credit_cost, reason]
        );
    } catch ( err ) {
        console.error( '[Credits] Refund error:', err.message );
    }
}

// ─────────────────────────────────────────────────────────────
// HTTP REQUEST — reusable for any provider
// ─────────────────────────────────────────────────────────────
async function providerRequest( poolName, account, endpoint, method = 'POST', data = null, isFormData = false ) {
    const pool    = POOLS[ poolName ];
    const url     = `${pool.baseUrl}${endpoint}`;
    const headers = {};

    if ( pool.authStyle === 'xi-api-key' ) {
        headers['xi-api-key'] = account.key;
    } else if ( pool.authStyle === 'bearer' ) {
        headers['Authorization'] = `Bearer ${account.key}`;
    }

    if ( isFormData && data instanceof FormData ) {
        Object.assign( headers, data.getHeaders() );
    } else if ( data ) {
        headers['Content-Type'] = 'application/json';
    }

    const response = await axios({
        method,
        url,
        headers,
        data:           isFormData ? data : ( data ? JSON.stringify( data ) : undefined ),
        timeout:        60000,
        validateStatus: null,
    });

    if ( response.status === 401 || response.status === 403 ) {
        await disableAccount( poolName, account.id, 3600 );
        throw new Error( `AUTH_FAILED:${response.status}` );
    }

    if ( response.status === 402 ) {
        await disableAccount( poolName, account.id, 86400 );
        throw new Error( `NO_CREDITS:${response.status}` );
    }

    return response.data;
}

// ─────────────────────────────────────────────────────────────
// JOB PROCESSORS
// ─────────────────────────────────────────────────────────────

async function processAudioTTS( job ) {
    const {
        prediction_id, queue_row_id, user_id,
        voice_id, text, model_id, output_format,
        voice_settings, voice_name, project_id, receive_url
    } = job.data;

    const account = await getAvailableAccount( 'ai33_audio' );
    if ( ! account ) throw new Error( 'NO_ACCOUNT_AVAILABLE:ai33_audio' );

    try {
        console.log( `[TTS] Processing ${prediction_id} via ${account.id}...` );
        await updateQueueRow( prediction_id, { status: 'processing' } );

        const res = await providerRequest( 'ai33_audio', account,
            `/v1/text-to-speech/${voice_id}?output_format=${output_format || 'mp3_44100_128'}`,
            'POST',
            {
                text,
                model_id:                 model_id || 'eleven_multilingual_v2',
                with_transcript:          false,
                voice_settings:           voice_settings || {},
                apply_text_normalization: 'auto',
                receive_url:              receive_url || WEBHOOK_URL,
            }
        );

        if ( ! res.success ) throw new Error( res.message || res.detail || 'TTS failed' );

        const taskId = res.task_id || res.data?.task_id;
        if ( ! taskId ) throw new Error( 'No task_id returned' );

        // Update by queue_row_id (reliable) not prediction_id (may mismatch)
await dbQuery(
    `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE id = ?`,
    [taskId, queue_row_id]
);
await dbQuery(
    `UPDATE ${TABLE_GENERATIONS} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE queue_id = ?`,
    [taskId, queue_row_id]
);

        console.log( `[TTS] ✅ Submitted. task_id: ${taskId}` );

    } catch ( err ) {
        console.error( `[TTS] ❌ Error: ${err.message}` );
        await dbQuery(
    `UPDATE ${TABLE_QUEUE} SET status = 'failed', error_message = ?, updated_at = NOW() WHERE id = ?`,
    [err.message, queue_row_id]
);
await dbQuery(
    `UPDATE ${TABLE_GENERATIONS} SET status = 'failed', updated_at = NOW() WHERE queue_id = ?`,
    [queue_row_id]
);
        await dbQuery(
            `UPDATE ${TABLE_GENERATIONS} SET status = 'failed', updated_at = NOW() WHERE prediction_id = ?`,
            [prediction_id]
        );
        await refundCredits( prediction_id, `tts_failed: ${err.message}` );
        await decrementCounters( user_id, 'audio_tts' );
        throw err;
    } finally {
        await releaseSlot( 'ai33_audio', account.id );
    }
}

async function processImageGenerate( job ) {
    const { prediction_id, user_id, provider, model_id, prompt } = job.data;
    const poolName = provider || 'ai33_image';
    const account  = await getAvailableAccount( poolName );
    if ( ! account ) throw new Error( `NO_ACCOUNT_AVAILABLE:${poolName}` );

    try {
        console.log( `[Image] Processing ${prediction_id} via ${account.id}...` );
        await updateQueueRow( prediction_id, { status: 'processing' } );

        const endpoint = job.data.endpoint || '/v1i/task/generate-image';
        const res      = await providerRequest( poolName, account, endpoint, 'POST', job.data.payload || {
            model_id,
            prompt,
            receive_url: WEBHOOK_URL,
        });

        if ( ! res.success ) throw new Error( res.message || 'Image generation failed' );

        const taskId = res.task_id || res.data?.task_id;
        await dbQuery(
            `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );

        console.log( `[Image] ✅ Submitted. task_id: ${taskId}` );

    } catch ( err ) {
        console.error( `[Image] ❌ Error: ${err.message}` );
        await updateQueueRow( prediction_id, { status: 'failed', error_message: err.message } );
        await refundCredits( prediction_id, `image_failed: ${err.message}` );
        await decrementCounters( user_id, 'image_generate' );
        throw err;
    } finally {
        await releaseSlot( poolName, account.id );
    }
}

async function processVideoGenerate( job ) {
    const { prediction_id, user_id, provider } = job.data;
    const poolName = provider || 'ai33_video';
    const account  = await getAvailableAccount( poolName );
    if ( ! account ) throw new Error( `NO_ACCOUNT_AVAILABLE:${poolName}` );

    try {
        console.log( `[Video] Processing ${prediction_id} via ${account.id}...` );
        await updateQueueRow( prediction_id, { status: 'processing' } );

        const endpoint = job.data.endpoint || '/v1i/task/generate-video';
        const res      = await providerRequest( poolName, account, endpoint, 'POST', job.data.payload || {} );

        if ( ! res.success ) throw new Error( res.message || 'Video generation failed' );

        const taskId = res.task_id || res.data?.task_id;
        await dbQuery(
            `UPDATE ${TABLE_QUEUE} SET prediction_id = ?, status = 'processing', updated_at = NOW() WHERE prediction_id = ?`,
            [taskId, prediction_id]
        );

        console.log( `[Video] ✅ Submitted. task_id: ${taskId}` );

    } catch ( err ) {
        console.error( `[Video] ❌ Error: ${err.message}` );
        await updateQueueRow( prediction_id, { status: 'failed', error_message: err.message } );
        await refundCredits( prediction_id, `video_failed: ${err.message}` );
        await decrementCounters( user_id, 'video_generate' );
        throw err;
    } finally {
        await releaseSlot( poolName, account.id );
    }
}

async function processMusicGenerate( job ) {
    console.log( '[Music] Music generation not yet implemented in worker' );
    throw new Error( 'Music generation not yet implemented' );
}

// ─────────────────────────────────────────────────────────────
// MAIN JOB DISPATCHER
// ─────────────────────────────────────────────────────────────
async function processJob( job ) {
    console.log( `[Worker] Job: ${job.name} | id: ${job.id}` );

    switch ( job.name ) {
        case 'audio_tts':          return await processAudioTTS( job );
        case 'audio_change_voice': return await processAudioTTS( job ); // TODO: separate handler
        case 'audio_dub':          return await processAudioTTS( job ); // TODO: separate handler
        case 'image_generate':     return await processImageGenerate( job );
        case 'video_generate':     return await processVideoGenerate( job );
        case 'music_generate':     return await processMusicGenerate( job );
        default:
            console.warn( `[Worker] Unknown job type: ${job.name}` );
    }
}

// ─────────────────────────────────────────────────────────────
// DB FALLBACK — recover orphaned jobs every 5 minutes
// ─────────────────────────────────────────────────────────────
async function pollDBFallback( queue ) {
    try {
        const rows = await dbQuery(
            `SELECT id, user_id, model_slug, payload, priority
             FROM ${TABLE_QUEUE}
             WHERE status = 'queued'
             AND created_at > DATE_SUB(NOW(), INTERVAL 30 MINUTE)
             ORDER BY priority DESC, created_at ASC
             LIMIT 10`
        );

        if ( ! rows.length ) return;

        console.log( `[Fallback] Found ${rows.length} orphaned jobs — recovering...` );

        for ( const row of rows ) {
            const payload = JSON.parse( row.payload || '{}' );
            // Map model_slug to correct job type
const modelSlugToJobType = {
    'change-voice': 'audio_change_voice',
    'dub':          'audio_dub',
};
const jobName = payload.job_type 
    || modelSlugToJobType[row.model_slug] 
    || (row.model_slug?.startsWith('tts-') ? 'audio_tts' : null)
    || (row.model_slug?.startsWith('image-') ? 'image_generate' : null)
    || (row.model_slug?.startsWith('video-') ? 'video_generate' : null)
    || 'audio_tts';

            await dbQuery(
                `UPDATE ${TABLE_QUEUE} SET status = 'processing', updated_at = NOW() WHERE id = ?`,
                [row.id]
            );

            await queue.add( jobName, {
                ...payload,
                prediction_id: `pending-${row.id}`,
                queue_row_id:  row.id,
            }, { priority: row.priority || 0 });

            console.log( `[Fallback] Recovered job ${row.id}: ${jobName}` );
        }
    } catch ( err ) {
        console.error( '[Fallback] DB poll error:', err.message );
    }
}

// ─────────────────────────────────────────────────────────────
// BULL BOARD DASHBOARD
// ─────────────────────────────────────────────────────────────
async function startDashboard( queue ) {
    if ( process.env.ENABLE_DASHBOARD !== 'true' ) return;

    const { createBullBoard } = await import( '@bull-board/api' );
    const { BullMQAdapter }   = await import( '@bull-board/api/bullMQAdapter.js' );
    const { ExpressAdapter }  = await import( '@bull-board/express' );
    const express             = ( await import( 'express' ) ).default;

    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath( '/dashboard' );
    createBullBoard({ queues: [new BullMQAdapter( queue )], serverAdapter });

    const app  = express();
    const port = process.env.PORT || 3000;

    app.use( '/dashboard', ( req, res, next ) => {
        const b64    = ( req.headers.authorization || '' ).split( ' ' )[1] || '';
        const [l, p] = Buffer.from( b64, 'base64' ).toString().split( ':' );
        if ( l === ( process.env.DASHBOARD_USER || 'admin' ) && p === ( process.env.DASHBOARD_PASS || 'admin' ) ) return next();
        res.set( 'WWW-Authenticate', 'Basic realm="AI SaaS Jobs"' );
        res.status( 401 ).send( 'Auth required' );
    });

    app.use( '/dashboard', serverAdapter.getRouter() );

    // Health endpoint — returns pool + platform stats
    app.get( '/health', async ( req, res ) => {
        const redis     = await getRedis().catch( () => null );
        const poolStats = {};

        for ( const [poolName, pool] of Object.entries( POOLS ) ) {
            poolStats[poolName] = {
                accounts: await Promise.all( pool.accounts.map( async acc => {
                    let slots    = 0;
                    let disabled = false;
                    if ( redis ) {
                        disabled = !! await redis.exists( `pool:disabled:${poolName}:${acc.id}` );
                        slots    = parseInt( await redis.get( `pool:slots:${poolName}:${acc.id}` ) ) || 0;
                    }
                    return {
                        id:       acc.id,
                        key:      acc.key.substring( 0, 8 ) + '...',
                        active:   slots,
                        limit:    pool.limit,
                        disabled: disabled,
                    };
                })),
            };
        }

        // Platform + pool counters
        const platform = {};
        if ( redis ) {
            platform.total_active = parseInt( await redis.get( 'ai_saas:platform:total_active' ) ) || 0;
            for ( const poolName of ['audio','image','video','music'] ) {
                platform[`pool_${poolName}`] = parseInt( await redis.get( `ai_saas:pool:${poolName}:active` ) ) || 0;
            }
        }

        res.json({ status: 'ok', pools: poolStats, platform });
    });

    app.listen( port, () => {
        console.log( `[Dashboard] http://localhost:${port}/dashboard` );
        console.log( `[Health]    http://localhost:${port}/health` );
    });
}

// ─────────────────────────────────────────────────────────────
// START
// ─────────────────────────────────────────────────────────────
async function start() {
    console.log( '[Worker] Starting AI SaaS Queue Worker...' );
    console.log( `[Worker] Redis: ${process.env.REDIS_HOST}:${process.env.REDIS_PORT}` );

    for ( const [name, pool] of Object.entries( POOLS ) ) {
        console.log( `[Pool] ${name}: ${pool.accounts.length} accounts, type: ${pool.type}, limit: ${pool.limit}` );
    }

    try {
        await dbQuery( 'SELECT 1' );
        console.log( '[Worker] MySQL connected ✅' );
    } catch ( err ) {
        console.error( '[Worker] MySQL failed:', err.message );
        process.exit( 1 );
    }

    try {
        const redis = await getRedis();
        await redis.ping();
        console.log( '[Worker] Redis connected ✅' );
    } catch ( err ) {
        console.error( '[Worker] Redis failed:', err.message );
        process.exit( 1 );
    }

    const concurrency = parseInt( process.env.WORKER_CONCURRENCY || '20' );

    const worker = new Worker( 'ai-saas-jobs', processJob, {
        connection:       REDIS_CONFIG,
        concurrency:      concurrency,
        removeOnComplete: { count: 10 },
        removeOnFail:     { count: 20 },
    });

    worker.on( 'completed', job => console.log( `[Worker] ✅ Done: ${job.name} | ${job.id}` ) );
    worker.on( 'failed',    ( job, err ) => console.error( `[Worker] ❌ Failed: ${job?.name} | ${err.message}` ) );
    worker.on( 'error',     err => console.error( '[Worker] Error:', err.message ) );

    const queue = new Queue( 'ai-saas-jobs', { connection: REDIS_CONFIG });
    await startDashboard( queue );

    setTimeout( () => pollDBFallback( queue ), 300000 );
    setInterval( () => pollDBFallback( queue ), 5 * 60 * 1000 );

    process.on( 'SIGTERM', async () => {
        console.log( '[Worker] Shutting down...' );
        await worker.close();
        await queue.close();
        process.exit( 0 );
    });

    process.on( 'SIGINT', async () => {
        await worker.close();
        process.exit( 0 );
    });

    console.log( `[Worker] ✅ Ready. Concurrency: ${concurrency}` );
}

start().catch( err => {
    console.error( '[Worker] Fatal:', err.message );
    process.exit( 1 );
});
