// Documentation: https://workers.cloudflare.com

async function sendNotification(message) {
  try {
    // Reference: https://pushover.net/api
    const response = await fetch("https://api.pushover.net/1/messages.json", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        token: "<PUSHOVER_TOKEN>",
        user: "<PUSHOVER_USER>",
        message: message,
        priority: 0,
      }),
    });
    if (!response.ok) {
      const text = await response.text();
      console.log(
        `Failed to send notification (response) ${text}, status: ${response.status}, ${response.statusText}`
      );
    }
  } catch (error) {
    console.log(`Failed to send notification (exception) ${error}`);
  }
}

async function redis(command, ...args) {
  let path = encodeURIComponent(command);
  for (var i = 0; i < args.length - 1; i++) {
    path += "/";
    path += encodeURIComponent(args[i]);
  }
  let body = args[args.length - 1];
  // https://github.com/nicolasff/webdis
  const response = await fetch(`https://<WEBDIS_URL>/${path}`, {
    method: "PUT",
    headers: {
      Authorization: `Basic ${btoa("<WEBDIS_USER>:<WEBDIS_PASSWORD>")}`,
    },
    body,
  });
  if (!response.ok) {
    throw new Error(
      `redis/${command} failed: ${response.status}, ${response.statusText}`
    );
  }
  const result = await response.json();
  const data = result[command];
  if (typeof data === "undefined") {
    throw new Error(
      `redis/${command} unexpected response: ${JSON.stringify(result)}`
    );
  }
  return data;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createJob(topic, data) {
  while (true) {
    // Create a unique job ID from the current time
    const timestamp = new Date()
      .toISOString()
      .replace(/[-:]/g, "")
      .replace(/\.(\d{3})Z$/, ".$1000Z");
    const job_id = `${timestamp}-${topic}`;

    // Try to atomically set the job data key only if it does not exist
    const setnx = await redis(
      "SETNX",
      `job_data:${job_id}`,
      JSON.stringify({
        job_id,
        data,
      })
    );
    if (setnx) {
      // Add to tail (right) of incoming queue
      const rpush = await redis("RPUSH", `incoming:${topic}`, job_id);
      console.log(`rpush:${rpush}`);
      return job_id;
    }

    // Sleep for a short time before trying again
    await sleep(1);
  }
}

export default {
  async fetch(request, env, ctx) {
    if (request.method !== "POST") {
      return new Response("Unsupported method", {
        status: 405,
      });
    }

    // Check authorization
    const authHeader = request.headers.get("Authorization");
    const expectedAuth = "Bearer <WORKER_AUTH_TOKEN>";
    if (authHeader !== expectedAuth) {
      return new Response("Unauthorized", {
        status: 403,
      });
    }

    // Extract the topic
    const url = new URL(request.url);
    const topic = url.searchParams.get("topic");
    if (topic === null || topic === "") {
      return new Response("Missing topic", {
        status: 400,
      });
    }

    // Get the data from the request body
    let data;
    const contentType = request.headers.get("Content-Type");
    if (contentType && contentType.startsWith("text/plain")) {
      data = await request.text();
    } else {
      try {
        data = await request.json();
      } catch (error) {
        return new Response("Invalid JSON body", {
          status: 400,
        });
      }
    }

    // Create the job
    try {
      const job_id = await createJob(topic, data);
      return new Response(job_id);
    } catch (error) {
      const time = new Date().toISOString();
      await sendNotification(
        `[${time}] topic=${topic}, body=${JSON.stringify(data)} error: ${error}`
      );
    }
  },
};
