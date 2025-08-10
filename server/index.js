const express = require('express');
const { Pool } = require('pg');

const app = express();
const port = process.env.PORT || 3000;

// The database URL will be provided by Fly.io as an environment variable
const dbUrl = process.env.DATABASE_URL;

// Create a new PostgreSQL connection pool
const pool = new Pool({
  connectionString: dbUrl,
});

app.get('/', async (req, res) => {
  try {
    const client = await pool.connect();
    const result = await client.query('SELECT NOW()');
    client.release();
    res.json({
      message: 'Hello from the backend!',
      databaseTime: result.rows[0].now,
    });
  } catch (err) {
    console.error('Database connection error', err);
    res.status(500).send('Database connection error');
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
