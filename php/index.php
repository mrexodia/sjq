<?php

// Dependencies
require "vendor/autoload.php";

// Credentials
require ".credentials.php";

/**
 * Creates an sjq job and returns the job id
 */
function create_job($redis, $topic, $job_data)
{
    while (true) {
        // Create a unique job ID from the current time
        $timestamp = (new DateTime("now", new DateTimeZone("UTC")))->format("Ymd\THis.u\Z");
        $job_id    = "{$timestamp}-{$topic}";

        // Try to atomically set the job data key only if it does not exist
        $job_message = [
            "job_id" => $job_id,
            "data"   => $job_data,
        ];
        if ($redis->set("job_data:{$job_id}", json_encode($job_message), "nx")) {
            // Add to tail (right) of incoming queue
            $redis->rpush("incoming:{$topic}", $job_id);
            return $job_id;
        }

        // Sleep for a short time before trying again
        usleep(1);
    }
}

if (
    (isset($_SERVER["PHP_AUTH_USER"]) && ($_SERVER["PHP_AUTH_USER"] === $http_user)) &&
    (isset($_SERVER["PHP_AUTH_PW"]) && ($_SERVER["PHP_AUTH_PW"] === $http_pass))
) {
    $method = $_SERVER["REQUEST_METHOD"];
    if ($method === "POST") {
        try {
            // Get the topic from the URL parameter, error if not present
            if (! isset($_GET["topic"]) || $_GET["topic"] === "") {
                http_response_code(400);
                die("?topic is required!");
            }
            $topic = $_GET["topic"];

            // Read JSON body
            $body = file_get_contents("php://input");
            $data = json_decode($body, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                http_response_code(400);
                die("Body is not valid JSON!");
            }

            // Connect to Redis using predis
            $redis = new Predis\Client([
                "scheme"   => "tcp",
                "host"     => $redis_host,
                "port"     => $redis_port,
                "username" => $redis_user,
                "password" => $redis_pass,
            ]);

            // Create job using the create_job function
            $job_id = create_job($redis, $topic, $data);

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
