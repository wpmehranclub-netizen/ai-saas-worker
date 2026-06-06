<?php
/**
 * File: includes/class-queue.php
 *
 * Pushes jobs to Redis queue (BullMQ compatible format).
 * Worker on Railway reads these jobs and processes them.
 *
 * BullMQ job format:
 * Key: bull:{queue_name}:{job_id}
 * List: bull:{queue_name}:wait  ← jobs waiting to be picked up
 *
 * Usage:
 *   AI_Saas_Queue::push('audio_tts', ['prediction_id' => '...', 'text' => '...']);
 *   AI_Saas_Queue::push('audio_change_voice', [...]);
 *   AI_Saas_Queue::push('audio_dub', [...]);
 */

defined( 'ABSPATH' ) || exit;

class AI_Saas_Queue {

    const QUEUE_NAME = 'ai-saas-jobs';

    // ----------------------------------------------------------------
    // PUSH JOB TO REDIS
    // BullMQ compatible format
    // ----------------------------------------------------------------
    public static function push(
        string $job_name,
        array  $payload,
        int    $priority = 0,
        int    $max_attempts = 3
    ): bool {

        $redis = self::get_redis();
        if ( ! $redis ) {
            // Redis not configured — log and return false
            // Caller should handle direct API call as fallback
            error_log( '[AI SaaS Queue] Redis not configured — job not queued: ' . $job_name );
            return false;
        }

        try {
            $job_id    = uniqid( 'job_', true );
            $timestamp = round( microtime( true ) * 1000 ); // milliseconds

            // BullMQ job structure
            $job = [
                'id'        => $job_id,
                'name'      => $job_name,
                'data'      => $payload,
                'opts'      => [
                    'attempts'  => $max_attempts,
                    'backoff'   => [
                        'type'  => 'exponential',
                        'delay' => 1000,
                    ],
                    'priority'  => $priority,
                    'timestamp' => $timestamp,
                ],
                'timestamp' => $timestamp,
                'delay'     => 0,
                'priority'  => $priority,
                'attempts'  => 0,
                'stacktrace'=> [],
                'returnvalue'=> null,
                'failedReason'=> null,
            ];

            $queue_key    = 'bull:' . self::QUEUE_NAME;
            $job_key      = $queue_key . ':' . $job_id;
            $wait_list    = $queue_key . ':wait';
            $meta_key     = $queue_key . ':meta';
            $events_key   = $queue_key . ':events';

            // Store job data as hash (BullMQ format)
            $redis->hmset( $job_key, [
                'id'           => $job_id,
                'name'         => $job_name,
                'data'         => wp_json_encode( $payload ),
                'opts'         => wp_json_encode( $job['opts'] ),
                'timestamp'    => $timestamp,
                'delay'        => 0,
                'priority'     => $priority,
                'attempts'     => 0,
                'processedOn'  => 0,
                'finishedOn'   => 0,
                'stacktrace'   => '[]',
                'returnvalue'  => 'null',
                'failedReason' => '',
            ] );

            // Add to wait list — worker picks from here
            if ( $priority > 0 ) {
                // Priority queue — use sorted set
                $redis->zadd( $queue_key . ':priority', $priority, $job_id );
            } else {
                // Standard FIFO queue
                $redis->lpush( $wait_list, $job_id );
            }

            // Update queue meta
            $redis->hincrby( $meta_key, 'jobCount', 1 );

            error_log( '[AI SaaS Queue] Job pushed: ' . $job_name . ' | id: ' . $job_id );
            return true;

        } catch ( Exception $e ) {
            error_log( '[AI SaaS Queue] Push failed: ' . $e->getMessage() );
            return false;
        }
    }

    // ----------------------------------------------------------------
    // GET QUEUE STATS — for admin dashboard
    // ----------------------------------------------------------------
    public static function get_stats(): array {
        $redis = self::get_redis();
        if ( ! $redis ) {
            return [
                'waiting'   => 0,
                'active'    => 0,
                'completed' => 0,
                'failed'    => 0,
                'redis'     => false,
            ];
        }

        try {
            $queue_key = 'bull:' . self::QUEUE_NAME;

            return [
                'waiting'   => (int) $redis->llen( $queue_key . ':wait' ),
                'active'    => (int) $redis->llen( $queue_key . ':active' ),
                'completed' => (int) $redis->zcard( $queue_key . ':completed' ),
                'failed'    => (int) $redis->zcard( $queue_key . ':failed' ),
                'redis'     => true,
            ];
        } catch ( Exception $e ) {
            error_log( '[AI SaaS Queue] Stats error: ' . $e->getMessage() );
            return [
                'waiting'   => 0,
                'active'    => 0,
                'completed' => 0,
                'failed'    => 0,
                'redis'     => false,
            ];
        }
    }

    // ----------------------------------------------------------------
    // IS REDIS CONFIGURED?
    // ----------------------------------------------------------------
    public static function is_configured(): bool {
        $config = file_exists( AI_SAAS_CONFIG ) ? include AI_SAAS_CONFIG : [];
        return ! empty( $config['redis_host'] ) && ! empty( $config['redis_pass'] );
    }

    // ----------------------------------------------------------------
    // GET REDIS CONNECTION
    // Uses phpredis extension (faster) or falls back to Predis
    // ----------------------------------------------------------------
    private static ?object $redis_instance = null;

    private static function get_redis(): ?object {
        if ( self::$redis_instance ) {
            return self::$redis_instance;
        }

        $config = file_exists( AI_SAAS_CONFIG ) ? include AI_SAAS_CONFIG : [];

        $host = $config['redis_host'] ?? '';
        $port = (int) ( $config['redis_port'] ?? 6379 );
        $pass = $config['redis_pass'] ?? '';
        $user = $config['redis_user'] ?? 'default';
        $tls  = (bool) ( $config['redis_tls'] ?? true );

        if ( empty( $host ) ) {
            return null;
        }

        // Try phpredis extension first (faster, C extension)
        if ( extension_loaded( 'redis' ) ) {
            try {
                $redis = new Redis();

                if ( $tls ) {
                    $redis->connect( 'tls://' . $host, $port, 5 );
                } else {
                    $redis->connect( $host, $port, 5 );
                }

                if ( ! empty( $pass ) ) {
                    if ( ! empty( $user ) && $user !== 'default' ) {
                        $redis->auth( [ $user, $pass ] );
                    } else {
                        $redis->auth( $pass );
                    }
                }

                $redis->ping();
                self::$redis_instance = $redis;
                return $redis;

            } catch ( Exception $e ) {
                error_log( '[AI SaaS Queue] phpredis connection failed: ' . $e->getMessage() );
                return null;
            }
        }

        // Fallback: try Predis (pure PHP, install via Composer)
        if ( class_exists( 'Predis\Client' ) ) {
            try {
                $scheme = $tls ? 'tls' : 'tcp';
                $redis  = new Predis\Client( [
                    'scheme'   => $scheme,
                    'host'     => $host,
                    'port'     => $port,
                    'username' => $user,
                    'password' => $pass,
                    'timeout'  => 5,
                ] );

                $redis->ping();
                self::$redis_instance = $redis;
                return $redis;

            } catch ( Exception $e ) {
                error_log( '[AI SaaS Queue] Predis connection failed: ' . $e->getMessage() );
                return null;
            }
        }

        error_log( '[AI SaaS Queue] No Redis client available. Install phpredis extension or Predis.' );
        return null;
    }

    // ----------------------------------------------------------------
    // RESET CONNECTION — call if connection drops
    // ----------------------------------------------------------------
    public static function reset_connection(): void {
        self::$redis_instance = null;
    }
}
