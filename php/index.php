<?php

// Dependencies
require('vendor/autoload.php');

// Credentials
require(".credentials.php");

/**
 * Creates an sjq job and returns the job id
 */
function create_job($redis, $topic, $job_data, $parent_job_id = null) {
    // Create a unique job ID from the current time
    while (true) {
        $timestamp = gmdate("Ymd\\TH:i:s.u\\Z");
        $job_id = "{$timestamp}-{$topic}";
        
        if ($redis->get("job_data:{$job_id}") === null) {
            break;
        }
        usleep(2000); // Sleep for 2ms (equivalent to Python's 0.002 seconds)
    }

    $job_message = [
        "job_id" => $job_id,
        "parent_job_id" => $parent_job_id,
        "data" => $job_data,
    ];
    
    // Store job data and add to incoming queue
    $redis->set("job_data:{$job_id}", json_encode($job_message));
    $redis->rpush("incoming:{$topic}", $job_id);

    return $job_id;
}

if (
    (isset($_SERVER['PHP_AUTH_USER']) && ($_SERVER['PHP_AUTH_USER'] === $http_user)) &&
    (isset($_SERVER['PHP_AUTH_PW']) && ($_SERVER['PHP_AUTH_PW'] === $http_pass))
) {
    $method = $_SERVER['REQUEST_METHOD'];
    if ($method === 'POST') {
        try {
            // Get the 'topic' from the URL parameter, error if not present
            if (!isset($_GET['topic']) || $_GET['topic'] === '') {
                http_response_code(400);
                die("?topic is required!");
            }
            $topic = $_GET['topic'];

            // Read JSON body
            $body = file_get_contents("php://input");
            $data = json_decode($body, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                http_response_code(400);
                die("Body is not valid JSON!");
            }

            // Connect to Redis using predis
            $redis = new Predis\Client([
                'scheme' => 'tcp',
                'host'   => $redis_host,
                'port'   => $redis_port,
                'username' => $redis_user,
                'password' => $redis_pass,
            ]);

            // Create job using the create_job function
            $parent_job_id = isset($_GET['parent_job_id']) ? $_GET['parent_job_id'] : null;
            $job_id = create_job($redis, $topic, $data, $parent_job_id);
            
            echo $job_id;
        } catch (Exception $e) {
            http_response_code(500);
            die($e->getMessage());
        }
    } else {
        http_response_code(405);
        print("Unsupported method " . $method);
    }
} else {
    header("WWW-Authenticate: Basic realm=\"sjq\"");
    header("HTTP/1.0 401 Unauthorized");
}
