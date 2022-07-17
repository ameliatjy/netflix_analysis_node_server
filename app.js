const pg = require('pg');
require('dotenv').config();
var path = require('path');

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

const app = express();
app.use(cors());
app.use(express.static(path.join(__dirname, 'public-flutter')));

/* Return data for a specific person */
app.get("/person/:type/:id", async (request, response) => {
  let id = request.params.id
  let type = request.params.type
  const query = {
    name: 'fetch-actor',
    text: `SELECT T.title, T.release_year, T.type, T.description, T.imdb_score, T.imdb_votes, T.tmdb_popularity, T.tmdb_score FROM credits C JOIN titles T ON C.id = T.id WHERE C.person_id = $1 AND C.role = $2 ORDER BY T.release_year asc`,
    values: [id, type],
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

/* Return data for top 10 actors */
app.get("/actors_analysis", async (request, response) => {
  client
    .query("SELECT C.person_id, C.name, MIN(CAST(T.release_year AS DECIMAL)), MAX(CAST(T.release_year AS DECIMAL)), COUNT(T.title), AVG(CAST(T.imdb_score AS DECIMAL)) AS avg_imdb_score, SUM(CAST(T.imdb_votes AS DECIMAL)) AS total_imdb_votes, AVG(CAST(T.tmdb_popularity AS DECIMAL)) AS avg_tmdb_popularity, AVG(CAST(T.tmdb_score AS DECIMAL)) AS avg_tmdb_score \
      FROM credits C JOIN titles T ON C.id = T.id WHERE C.role = 'ACTOR' GROUP BY C.person_id, C.name ORDER BY AVG(CAST(T.tmdb_popularity AS DECIMAL)) desc LIMIT 10")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

/* Return data for top 10 directors */
app.get("/directors_analysis", async (request, response) => {
  client
    .query("SELECT C.person_id, C.name, MIN(CAST(T.release_year AS DECIMAL)), MAX(CAST(T.release_year AS DECIMAL)), COUNT(T.title), AVG(CAST(T.imdb_score AS DECIMAL)) AS avg_imdb_score, SUM(CAST(T.imdb_votes AS DECIMAL)) AS total_imdb_votes, AVG(CAST(T.tmdb_popularity AS DECIMAL)) AS avg_tmdb_popularity, AVG(CAST(T.tmdb_score AS DECIMAL)) AS avg_tmdb_score \
      FROM credits C JOIN titles T ON C.id = T.id WHERE C.role = 'DIRECTOR' GROUP BY C.person_id, C.name HAVING COUNT(T.title) > 0 ORDER BY AVG(CAST(T.tmdb_popularity AS DECIMAL)) desc LIMIT 10")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

/* Return yearly average IMDB Score */
app.get("/analysis/imdb_score", async (request, response) => {
  client
    .query("SELECT AVG(CAST(T.imdb_score AS DECIMAL)) AS metric, T.release_year AS year \
      FROM titles T WHERE T.imdb_score IS NOT NULL GROUP BY T.release_year ORDER BY T.release_year")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

/* Return yearly average TMDB Score */
app.get("/analysis/tmdb_score", async (request, response) => {
  client
    .query("SELECT AVG(CAST(T.tmdb_score AS DECIMAL)) AS metric, T.release_year AS year \
      FROM titles T WHERE T.tmdb_score IS NOT NULL GROUP BY T.release_year ORDER BY T.release_year")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

/* Return yearly total runtime */
app.get("/analysis/runtime", async (request, response) => {
  client
    .query("SELECT SUM(CAST(T.runtime AS DECIMAL)) AS metric, T.release_year AS year \
      FROM titles T WHERE T.runtime IS NOT NULL GROUP BY T.release_year ORDER BY T.release_year")
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

/* Return total runtime for a genre */
app.get("/analysis/genres/:genre", async (request, response) => {
  let genre = request.params.genre
  const query = `SELECT SUM(CAST(T.runtime AS DECIMAL)) AS total_runtime
  FROM titles T WHERE T.runtime IS NOT NULL AND T.genres LIKE '%` + genre +`%'
  ORDER BY SUM(CAST(T.runtime AS DECIMAL))`
  client.query(query)
    .then(res => {
      const rows = res.rows;
      response.status(200).send(JSON.stringify(rows));
  })
  .catch(err => {
      console.log(err);
  });
});

module.exports = app;