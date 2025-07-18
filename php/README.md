# sjq php

Install the dependencies:

```sh
composer install
```

Then copy all the files to your PHP server to allow the following:

```http
POST https://example.com/sjq/index.php?topic=test
Authorization: Basic http-user:http-password

{
    "url": "https://www.cs.virginia.edu/~robins/YouAndYourResearch.html"
}
```

This is equivalent to `sjq create test` with the payload JSON.

## Cloudflare Workers

Alternatively you can deploy [webdis](https://github.com/nicolasff/webdis) and set up a Cloudflare worker based on `cf-worker.js`.
