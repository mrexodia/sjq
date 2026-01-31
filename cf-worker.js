// Documentation: https://workers.cloudflare.com
const PUSHOVER_TOKEN = "<PUSHOVER_TOKEN>";
const PUSHOVER_USER = "<PUSHOVER_USER>";
const WEBDIS_ENDPOINT = "https://webdis.example.com";
const WEBDIS_HEADERS = {
  Authorization: `Basic ${btoa("default:password")}`,
  "CF-Access-Client-Id": "<CF_ACCESS_CLIENT_ID>",
  "CF-Access-Client-Secret": "<CF_ACCESS_CLIENT_SECRET>",
};
const AUTH_BEARER_TOKEN = "<AUTH_BEARER_TOKEN>";

async function sendNotification(message) {
  try {
    // Reference: https://pushover.net/api
    const response = await fetch("https://api.pushover.net/1/messages.json", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        token: PUSHOVER_TOKEN,
        user: PUSHOVER_USER,
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
  // https://webd.is/
  const response = await fetch(`${WEBDIS_ENDPOINT}/${path}`, {
    method: "PUT",
    headers: WEBDIS_HEADERS,
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

function getTimestamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.(\d{3})Z$/, ".$1000Z");
}

async function createJob(topic, data, attachment) {
  while (true) {
    // Create a unique job ID from the current time
    const timestamp = getTimestamp();
    const job_id = `${timestamp}:${topic}`;

    // Try to atomically set the job data key only if it does not exist
    const setnx = await redis(
      "SETNX",
      `job_data:${job_id}`,
      JSON.stringify({
        job_id,
        data,
        attachment: attachment ? true : false,
      })
    );
    if (setnx) {
      if (attachment) {
        await redis("SET", `job_attachment:${job_id}`, attachment);
      }

      // Add to tail (right) of incoming queue
      const rpush = await redis("RPUSH", `incoming:${topic}`, job_id);
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
    const expectedAuth = `Bearer ${AUTH_BEARER_TOKEN}`;
    if (authHeader !== expectedAuth) {
      return new Response("Unauthorized", {
        status: 403,
      });
    }

    // Extract the topic
    const url = new URL(request.url);
    const topic = url.pathname.replaceAll("/", ":").substring(1);
    if (!topic) {
      return new Response("Missing topic", {
        status: 400,
      });
    }

    // Get the data from the request body
    let data;
    let attachment = null;
    const contentType = request.headers.get("Content-Type") || "";
    if (contentType.startsWith("multipart/form-data")) {
      const form = await request.formData();
      data = {};

      for (const [key, value] of form.entries()) {
        if (value instanceof File) {
          if (attachment) {
            return new Response("Too many files; only one attachment supported", {
              status: 400,
            });
          }
          attachment = await value.arrayBuffer();
        } else if (Object.prototype.hasOwnProperty.call(data, key)) {
          if (!Array.isArray(data[key])) {
            data[key] = [data[key]];
          }
          data[key].push(value);
        } else {
          data[key] = value;
        }
      }
    } else if (contentType.startsWith("application/json")) {
      try {
        data = await request.json();
      } catch (error) {
        return new Response("Invalid JSON body", {
          status: 400,
        });
      }
    } else if (contentType.startsWith("application/x-www-form-urlencoded")) {
      const bodyText = await request.text();
      const params = new URLSearchParams(bodyText);
      data = {};
      for (const [key, value] of params.entries()) {
        if (Object.prototype.hasOwnProperty.call(data, key)) {
          if (!Array.isArray(data[key])) {
            data[key] = [data[key]];
          }
          data[key].push(value);
        } else {
          data[key] = value;
        }
      }
    } else if (contentType.startsWith("text/plain")) {
      data = await request.text();
    } else if (contentType.startsWith("application/octet-stream")) {
      attachment = await request.arrayBuffer();
      data = Object.fromEntries(url.searchParams);
    } else {
      const buffer = await request.arrayBuffer();
      const decoder = new TextDecoder("utf-8", {
        fatal: true,
        ignoreBOM: false,
      });
      try {
        data = decoder.decode(buffer);
      } catch (error) {
        attachment = buffer;
        data = Object.fromEntries(url.searchParams);
      }
    }

    // Log all valid requests
    console.log(
      JSON.stringify({
        topic,
        data,
      })
    );

    // Create the job
    try {
      const job_id = await createJob(topic, data, attachment);
      return new Response(job_id);
    } catch (error) {
      const time = new Date().toISOString();
      console.error("Failed to create job", error);
      await sendNotification(
        `[${time}] topic=${topic}, body=${JSON.stringify(data)} error: ${error}`
      );
      return new Response("Failed to create job", { status: 500 });
    }
  },
};
