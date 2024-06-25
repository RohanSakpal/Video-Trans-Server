const express = require('express');
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');
const cors = require('cors');

const app = express();
const port = 3000;

app.use(cors());

// Call the function to decrypt .env.encrypted file


const projectId = 'ai-analytics-406109';
const datasetId = 'Atrina_DS';
const keyFilePath = path.join(__dirname, './assets/creads.json');

const bigquery = new BigQuery({
  projectId: projectId,
  keyFilename: keyFilePath,
});

app.get('/api/gcp-data', async (req, res) => {
  const tableId = 'pharma_vtt';
  const keyword = req.query.keyword || '';

  // const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE transcript LIKE '%${keyword}%'`;
  // const query = `WITH RankedResults AS (
  //     SELECT *,
  //            ROW_NUMBER() OVER (PARTITION BY v_id ORDER BY v_id) AS rn
  //     FROM \`${projectId}.${datasetId}.${tableId}\`
  //     WHERE LOWER(transcript) LIKE LOWER('%${keyword}%')
  //   )
  //   SELECT *
  //   FROM RankedResults
  //   WHERE rn = 1`;
  const query = `
    DECLARE search_keyword STRING;
    SET search_keyword = '${keyword.toLowerCase()}'; 

    WITH cte AS (
      SELECT *
      FROM \`${projectId}.${datasetId}.${tableId}\`
      WHERE SEARCH(
        LOWER(transcript),
        LOWER(search_keyword),
        analyzer_options => '{ "token_filters": [{"stop_words": ["the"]}] }'
      ) = TRUE OR
      SEARCH(LOWER(videoname), 
       LOWER(search_keyword), 
       analyzer_options => '{ "token_filters": [{"stop_words": ["the"]}] }'
       ) = True

    )
    SELECT DISTINCT v_id, category, videoname, video_link
    FROM cte
  `;

  try {
    const [rows] = await bigquery.query(query);
    res.status(200).json(rows);
  } catch (error) {
    console.error('ERROR:', error);
    res.status(500).send('Error querying BigQuery');
  }
});

// app.get('/api/gcp-data', async (req, res) => {
//   const tableId = 'pharma_vtt';
//   const keyword = req.query.keyword || '';

//   const query = `
//     DECLARE search_keyword STRING;
//     SET search_keyword = '${keyword.toLowerCase()}'; -- Example keyword with spelling mistake

//     WITH normalized_transcriptions AS (
//       SELECT
//         v_id,
//         category,
//         videoname,
//         video_link,
//         LOWER(transcript) AS transcription
//       FROM \`${projectId}.${datasetId}.${tableId}\`
//     ),

//     split_search_keyword AS (
//       SELECT
//         word,
//         SOUNDEX(word) AS soundex_search_word
//       FROM UNNEST(SPLIT(LOWER(search_keyword), ' ')) AS word
//     ),

//     split_transcriptions AS (
//       SELECT
//         v_id,
//         category,
//         videoname,
//         video_link,
//         transcription,
//         word,
//         SOUNDEX(word) AS soundex_transcription_word
//       FROM normalized_transcriptions,
//       UNNEST(SPLIT(transcription, ' ')) AS word
//     ),

//     matches AS (
//       SELECT
//         nt.v_id,
//         nt.category,
//         nt.videoname,
//         nt.video_link,
//         nt.transcription
//       FROM split_transcriptions st
//       JOIN normalized_transcriptions nt ON st.transcription = nt.transcription
//       JOIN split_search_keyword sk ON st.soundex_transcription_word = sk.soundex_search_word
//     )

//     SELECT DISTINCT v_id, category, videoname, video_link
//     FROM matches
//     GROUP BY 1, 2, 3, 4;
//   `;

//   try {
//     const [rows] = await bigquery.query({ query });
//     res.status(200).json(rows);
//   } catch (error) {
//     console.error('ERROR:', error);
//     res.status(500).send('Error querying BigQuery');
//   }
// });

// app.get('/api/video-data', async (req, res) => {
//   //const tableId = 'pharma_vtt_master';
//   const tableId = 'pharma_vtt';
//   const keyword = req.query.keyword || '';
//   const id = Number(req.query.id) || '';

//   const query = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE v_id = ${id} AND LOWER(transcript) LIKE LOWER('%${keyword}%') ORDER BY start ASC`;

//   try {
//     const [rows] = await bigquery.query(query);
//     res.status(200).json(rows);
//   } catch (error) {
//     console.error('ERROR:', error);
//     res.status(500).send('Error querying BigQuery');
//   }
// });

app.get('/api/video-data', async (req, res) => {
  const tableId = 'pharma_vtt';
  const keyword = req.query.keyword || '';
  const id = Number(req.query.id) || '';

  //const matchTrascQuery = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE v_id = ${id} AND LOWER(transcript) LIKE LOWER('%${keyword}%') ORDER BY start ASC`;
  const matchTrascQuery = `
    DECLARE search_keyword STRING;
SET search_keyword = '${keyword.toLowerCase()}';

WITH cte AS (
  SELECT *
  FROM \`${projectId}.${datasetId}.${tableId}\`
  WHERE SEARCH(
    LOWER(transcript),
    LOWER(search_keyword),
    analyzer_options => '{ "token_filters": [{"stop_words": ["the"]}] }'
  ) = TRUE
)
SELECT v_id, category, videoname, video_link, transcript, start,\`end\`
FROM cte
WHERE v_id = ${id}
`;

// OR SEARCH(
//   LOWER(videoname), 
//   LOWER(search_keyword), 
//   analyzer_options => '{ "token_filters": [{"stop_words": ["the"]}] }'
//   ) = True
  const fullTransquery = `SELECT * FROM \`${projectId}.${datasetId}.${tableId}\` WHERE v_id = ${id} ORDER BY start ASC`;

  try {
    // Execute both queries concurrently
    const [matchTrascResult, fullTransResult] = await Promise.all([
      bigquery.query({ query: matchTrascQuery }),
      bigquery.query({ query: fullTransquery })
    ]);

    // Structure the response
    const response = {
      MatchTrascQuery: matchTrascResult[0], // Extract rows from query result
      FullTransquery: fullTransResult[0] // Extract rows from query result
    };

    // Send the response
    res.status(200).json(response);
  } catch (error) {
    console.error('ERROR:', error);
    res.status(500).send('Error querying BigQuery');
  }
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
