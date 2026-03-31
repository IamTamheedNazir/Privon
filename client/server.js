const express = require("express");

const { search } = require("./searchClient");

const app = express();
const port = Number(process.env.PORT || 3000);
const coordinatorUrl = process.env.COORDINATOR_URL || "http://localhost:4000";

app.use(express.json());

app.get("/health", (_request, response) => {
  response.json({
    status: "ok",
    coordinatorUrl,
  });
});

app.post("/search", async (request, response) => {
  const query = request.body?.query;
  const limit = Number(request.body?.limit || 5);

  if (!query || typeof query !== "string") {
    return response.status(400).json({
      error: "A non-empty query string is required.",
    });
  }

  try {
    const result = await search(query, {
      coordinatorUrl,
      limit,
    });

    return response.json(result);
  } catch (error) {
    return response.status(502).json({
      error: error.message,
    });
  }
});

app.listen(port, () => {
  console.log(`[client] listening on port ${port} and forwarding to ${coordinatorUrl}`);
});
