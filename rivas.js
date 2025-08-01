const express = require("express");
const { MongoClient } = require("mongodb");
const axios = require("axios");

const app = express();
app.use(express.json());

let rivasDb;

// Identity service helper
const identityAPI = "http://localhost:5000";

// Helper: Check if email exists globally
const emailExists = async (email) => {
  const response = await axios.get(`${identityAPI}/check`, {
    params: { email },
  });
  return response.data.exists;
};

// Signup user: only insert into LMDB through identity API
app.post("/signup", async (req, res) => {
  const { email, name, age } = req.body;

  if (!email || !name || !age) {
    return res.status(400).json({ error: "Missing fields." });
  }

  try {
    // Check if email already exists
    const exists = await emailExists(email);
    if (exists) {
      return res.status(409).json({ error: "Email already exists." });
    }

    const userId = `rivas-${Date.now().toString(36)}`;
    const user = { userId, email, name, age, app: "rivas" };

    // Post user to the identity API's LMDB write endpoint
    try {
      await axios.post(
        `${identityAPI}/identity`,
        { user },
        { headers: { "x-app-name": "rivas" } }
      );

      return res.status(201).json({ message: "User created via cache", user });
    } catch (apiErr) {
      console.error("❌ Failed to write to identity API:", apiErr.message);
      return res.status(502).json({
        error: "Failed to write to identity API",
        details: apiErr.message,
      });
    }
  } catch (err) {
    console.error("❌ Signup failed:", err.message);
    return res
      .status(500)
      .json({ error: "Signup failed", details: err.message });
  }
});

// Login
app.post("/login", async (req, res) => {
  const { email } = req.body;
  try {
    const response = await axios.post(
      `${identityAPI}/auth`,
      { email },
      { headers: { "x-app-name": "rivas" } }
    );
    res.json({ user: response.data.user });
  } catch (err) {
    res.status(500).json({ error: "Login failed", details: err.message });
  }
});

// Profile retrieval
app.post("/profile", async (req, res) => {
  try {
    const response = await axios.post(
      `${identityAPI}/identity`,
      { user: req.body },
      { headers: { "x-app-name": "rivas" } }
    );
    res.json({ user: response.data.user });
  } catch (err) {
    res
      .status(500)
      .json({ error: "Profile fetch failed", details: err.message });
  }
});

// App-specific endpoints (watchlist, rating, etc.)
app.post("/add-to-watchlist", async (req, res) => {
  const { userId, movieId } = req.body;
  await rivasDb.collection("watchlist").insertOne({ userId, movieId });
  res.json({ message: "Added to watchlist." });
});

app.post("/update-watch-progress", async (req, res) => {
  const { userId, movieId, progress } = req.body;
  await rivasDb
    .collection("progress")
    .updateOne({ userId, movieId }, { $set: { progress } }, { upsert: true });
  res.json({ message: "Progress updated." });
});

app.post("/rate-movie", async (req, res) => {
  const { userId, movieId, rating } = req.body;
  await rivasDb.collection("ratings").insertOne({ userId, movieId, rating });
  res.json({ message: "Rating recorded." });
});

// Simulated stream
app.get("/stream/:movieId", (req, res) => {
  res.json({ message: `Streaming movie: ${req.params.movieId}` });
});

// Startup
(async () => {
  const client = new MongoClient("mongodb://localhost:27017");
  await client.connect();
  rivasDb = client.db("rivas_db");
  app.listen(7000, () => console.log("Rivas running on http://localhost:7000"));
})();
