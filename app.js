const pg = require('pg');
require('dotenv').config();

const config = {
    host: 'netflixanalysisserver.postgres.database.azure.com',
    user: process.env.AZURE_USER,     
    password: process.env.AZURE_PW,
    database: 'postgres',
    port: 5432,
    ssl: true
};

const client = new pg.Client(config);

client.connect(err => {
  if (err) throw err;
});

const express = require('express');
var cors = require('cors')
const PORT = 8000;

const app = express();

app.use(cors());

app.get("/actor/:id", async (request, response) => {
  let id = request.params.id.replace(/\n/g, '')
  const query = {
    // give the query a unique name
    name: 'fetch-actor',
    text: `SELECT T.title, T.release_year, T.type, T.description, T.imdb_score, T.imdb_votes, T.tmdb_popularity, T.tmdb_score FROM credits C JOIN titles T ON C.id = T.id WHERE T.id = $1 AND C.role = 'ACTOR' ORDER BY T.release_year asc`,
    values: [id],
  }
  client.query(query)
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

app.get("/actors_analysis", async (request, response) => {
  client.query("SELECT C.name, MIN(CAST(T.release_year AS DECIMAL)), MAX(CAST(T.release_year AS DECIMAL)), COUNT(T.title), AVG(CAST(T.imdb_score AS DECIMAL)) AS avg_imdb_score, SUM(CAST(T.imdb_votes AS DECIMAL)) AS total_imdb_votes, AVG(CAST(T.tmdb_popularity AS DECIMAL)) AS avg_tmdb_popularity, AVG(CAST(T.tmdb_score AS DECIMAL)) AS avg_tmdb_score FROM credits C JOIN titles T ON C.id = T.id WHERE C.role = 'ACTOR' GROUP BY C.person_id, C.name")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

app.get("/directors_analysis", async (request, response) => {
  client.query("SELECT C.name, MIN(CAST(T.release_year AS DECIMAL)), MAX(CAST(T.release_year AS DECIMAL)), COUNT(T.title), AVG(CAST(T.imdb_score AS DECIMAL)) AS avg_imdb_score, SUM(CAST(T.imdb_votes AS DECIMAL)) AS total_imdb_votes, AVG(CAST(T.tmdb_popularity AS DECIMAL)) AS avg_tmdb_popularity, AVG(CAST(T.tmdb_score AS DECIMAL)) AS avg_tmdb_score FROM credits C JOIN titles T ON C.id = T.id WHERE C.role = 'DIRECTOR' GROUP BY C.person_id, C.name")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

app.listen(PORT, () => {
  console.log(`connected at port ${PORT}`)
})