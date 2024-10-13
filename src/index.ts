/* eslint-disable @typescript-eslint/use-unknown-in-catch-callback-variable */

/* eslint-disable @typescript-eslint/no-unused-vars */

/* eslint-disable @typescript-eslint/no-unsafe-assignment */
import { AtpAgent } from '@atproto/api';
import { cborToLexRecord, readCarWithRoot, verifyRepo } from '@atproto/repo';
import express from 'express';
import { Registry, collectDefaultMetrics } from 'prom-client';

const register = new Registry();
collectDefaultMetrics({ register });

const app = express();

app.get('/metrics', (req, res) => {
  register
    .metrics()
    .then((metrics) => {
      res.set('Content-Type', register.contentType);
      res.send(metrics);
    })
    .catch((ex: unknown) => {
      console.error(`Error serving metrics: ${(ex as Error).message}`);
      res.status(500).end((ex as Error).message);
    });
});

const server = app.listen(9999, '127.0.0.1', () => {
  console.log(`Metrics server listening on port 9999`);
});

server.on('close', () => {
  console.log('Metrics server closed.');
});

const RELAY_URL = 'https://bsky.network';
const relayAgent = new AtpAgent({ service: RELAY_URL });

let processedRepoCount = 0;
async function processRepo({ did }: { did: string }) {
  try {
    processedRepoCount += 1;
    if (processedRepoCount % 10 === 0) {
      console.log('Processed ', processedRepoCount, ' repos in processRepo()');
    }
    try {
      const fetchStartTime = performance.now();
      const res = await relayAgent.com.atproto.sync.getRepo({ did: did });
      const fetchEndTime = performance.now();
      console.log(`getRepo time for ${did}: ${(fetchEndTime - fetchStartTime).toFixed(0)}ms`);

      if (!res.success) {
        console.error(`Failed to getRepo ${did}`, res);
        return;
      }

      const readStartTime = performance.now();

      const { root, blocks } = await readCarWithRoot(res.data);
      const readEndTime = performance.now();
      console.log(`Read CAR time for ${did}: ${(readEndTime - readStartTime).toFixed(0)}ms`);

      const verifyStartTime = performance.now();
      const repo = await verifyRepo(blocks, root, did);
      const verifyEndTime = performance.now();
      console.log(`Verify repo time for ${did}: ${(verifyEndTime - verifyStartTime).toFixed(0)}ms`);

      const processStartTime = performance.now();
      let processedRecords = 0;

      for (const { collection, rkey, cid } of repo.creates) {
        const uri = `at://${did}/${collection}/${rkey}`;

        const recordBytes = blocks.get(cid);
        if (!recordBytes) continue;
        const record = cborToLexRecord(recordBytes);

        processedRecords++;
      }

      const processEndTime = performance.now();
      console.log(
        `Processed ${processedRecords} records for ${did} in ${(processEndTime - processStartTime).toFixed(0)}ms`,
      );
    } catch (e) {
      console.error(`Failed to process repo ${did}`, e);
    }
  } catch (error) {
    console.error(`Failed to process repo ${did}`, error);
  }
}

async function backfill() {
  const res = await fetch(`${RELAY_URL}/xrpc/com.atproto.sync.listRepos`);
  const data = await res.json();
  const { repos } = data as { repos: unknown[] };

  for (const repo of repos) {
    if (typeof repo !== 'object' || !repo || !['did', 'head', 'rev'].every((k) => k in repo)) {
      console.error(`Invalid repo object`, repo);
      continue;
    }
    try {
      await processRepo(repo as { did: string });
    } catch (error: unknown) {
      console.error(`Failed to process repo`, error);
    }
  }
}

backfill().catch(console.error);
