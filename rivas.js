const express = require("express");
const { MongoClient } = require("mongodb");
const axios = require("axios");
const dotenv = require("dotenv");
dotenv.config();
const app = express();
app.set("trust proxy", 1);
app.use(express.json());
const Aerospike = require('aerospike');

let rivasDb;
let aerospikeClient;
// Identity service helper
const identityAPI = "https://aerospike.brivas.io/api/identity";

const aerospikeConfig = {
  hosts: process.env.AEROSPIKE_HOSTS, // e.g. "localhost:3000"
};

// Helper: Aerospike user key
const userKey = (email) => ({ ns: 'rivas', set: 'users', key: email });

// Helper: Check if email exists globally (identity API)
const emailExists = async (email) => {
  const response = await axios.get(`${identityAPI}/check`, {
    params: { email },
    headers: { "x-app-name": "rivas" },
  });
  return response.data.exists;
};

// Signup user: insert into Aerospike and identity API
app.post("/signup", async (req, res) => {
  const { email, name, age } = req.body;

  if (!email || !name || !age) {
    return res.status(400).json({ error: "Missing fields." });
  }

  try {
    // Check if email already exists globally
    const exists = await emailExists(email);
    if (exists) {
      return res.status(409).json({ error: "Email already exists." });
    }

    // Check if user exists in Aerospike
    try {
      await aerospikeClient.get(userKey(email));
      return res.status(409).json({ error: "Email already exists (Aerospike)." });
    } catch (err) {
      if (err.code !== Aerospike.status.AEROSPIKE_ERR_RECORD_NOT_FOUND) {
        throw err;
      }
    }

    const userId = `rivas-${Date.now().toString(36)}`;
    const user = { userId, email, name, age, app: "rivas" };

    // Write to Aerospike
    await aerospikeClient.put(userKey(email), user);

    // Optionally, also post to the identity API
    try {
      await axios.post(
        identityAPI,
        { user },
        { headers: { "x-app-name": "rivas" } }
      );
    } catch (apiErr) {
      console.error("❌ Failed to write to identity API:", apiErr.message);
      // Still return success, since Aerospike write succeeded
      return res.status(201).json({
        message: "User created in Aerospike, but failed to write to identity API",
        user,
        identityApiError: apiErr.message,
      });
    }

    return res.status(201).json({ message: "User created", user });
  } catch (err) {
    console.error("❌ Signup failed:", err.message);
    return res
      .status(500)
      .json({ error: "Signup failed", details: err.message });
  }
});

// Login: fetch from Aerospike
app.post("/login", async (req, res) => {
  const { email } = req.body;
  try {
    const record = await aerospikeClient.get(userKey(email));
    res.json({ user: record.bins });
  } catch (err) {
    res.status(404).json({ error: "User not found", details: err.message });
  }
});

// Profile retrieval: fetch from Aerospike
app.post("/profile", async (req, res) => {
  const { email } = req.body;
  try {
    const record = await aerospikeClient.get(userKey(email));
    res.json({ user: record.bins });
  } catch (err) {
    res.status(404).json({ error: "User not found", details: err.message });
  }
});

// App-specific endpoints (watchlist, rating, etc.) remain in MongoDB
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
  // Connect to MongoDB
  const client = new MongoClient(process.env.MONGO_URI);
  await client.connect();
  rivasDb = client.db("rivas_db");

  // Connect to Aerospike
  aerospikeClient = await Aerospike.connect(aerospikeConfig);

  app.listen(7000, () => console.log("Rivas running on http://localhost:7000"));
})();