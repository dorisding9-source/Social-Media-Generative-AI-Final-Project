/**
 * Agent 1 — The Historian (waterfall):
 * 1) Web search + fetch CSV/JSON archives (Serper or Brave API)
 * 2) Twitter/X API v2 (+ optional Apify if API fails)
 * 3) Playwright scroll + Claude Vision (stance_watch / marketing_agent pattern)
 */
import {
  clearTweetsForHandle,
  insertTweet,
  getCorpusMeta,
  saveCorpusMeta,
  getTweetsForHandle,
  getAnalysis,
} from "../db/database.js";
import { lookupTwitterProfileByUsername } from "../lib/twitterUser.js";
import { withRetries } from "../lib/retry.js";
import { searchWeb } from "../lib/webSearch.js";
import { discoverAndIngestArchives } from "../lib/archiveIngest.js";
import { runScreenshotVisionScrape } from "./screenshotVisionScrape.js";
import { fallbackAvatarUrl } from "../lib/avatars.js";

const THREE_YEARS_MS = 3 * 365 * 24 * 60 * 60 * 1000;
const MIN_TWEETS_OK = 72;
const MIN_CORPUS_SPAN_DAYS = 30;

function corpusSourceTrusted(source) {
  if (!source) return false;
  if (source === "mock" || source === "mock_fallback") return false;
  return true;
}

function corpusSpanDays(oldestIso, newestIso) {
  if (!oldestIso || !newestIso) return 0;
  const a = new Date(oldestIso).getTime();
  const b = new Date(newestIso).getTime();
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  return Math.max(0, (b - a) / 86400000);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function mockProfile(handle) {
  const h = handle.replace(/^@/, "").toLowerCase();
  return {
    id: "mock_user",
    name: h,
    username: h,
    description: "Synthetic profile for mock data mode.",
    profile_image_url: fallbackAvatarUrl(h),
  };
}

function mockTweets(handle, count = 240) {
  const h = handle.replace(/^@/, "").toLowerCase();
  const now = Date.now();
  const topicsA = [
    "Markets and innovation drive progress.",
    "We need pragmatic regulation, not overreach.",
    "Energy abundance matters for working families.",
  ];
  const topicsB = [
    "Solidarity and public investment define justice.",
    "Corporate power must be checked by democratic institutions.",
    "Climate action requires collective responsibility, not individual blame.",
  ];
  const out = [];
  for (let i = 0; i < count; i++) {
    const t = now - (count - i) * (THREE_YEARS_MS / count);
    const iso = new Date(t).toISOString();
    const phase = i < count * 0.72 ? topicsA : topicsB;
    const text = phase[Math.floor(Math.random() * phase.length)];
    out.push({
      id: `mock_${h}_${i}`,
      tweet_text: `${text} #thread ${i}`,
      created_at: iso,
      likes: 10 + (i % 50),
      retweets: 2 + (i % 20),
      replies: 1 + (i % 10),
    });
  }
  return out;
}

async function twitterFetch(path, bearer) {
  const res = await fetch(`https://api.twitter.com/2/${path}`, {
    headers: { Authorization: `Bearer ${bearer}` },
  });
  if (res.status === 429) {
    const reset = res.headers.get("x-rate-limit-reset");
    const waitSec = reset ? Math.max(1, Number(reset) - Math.floor(Date.now() / 1000)) : 5;
    await sleep(Math.min(waitSec * 1000, 90_000));
    throw new Error("Twitter rate limited — retrying");
  }
  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Twitter API ${res.status}: ${errText.slice(0, 400)}`);
  }
  return res.json();
}

async function fetchUserByUsername(username, bearer) {
  const u = encodeURIComponent(username.replace(/^@/, ""));
  const data = await twitterFetch(`users/by/username/${u}?user.fields=profile_image_url,description,name,username`, bearer);
  if (!data.data) throw new Error("User not found on Twitter/X");
  return data.data;
}

async function fetchAllTweets(userId, bearer, startIso) {
  const tweets = [];
  let token = null;
  const fields = "created_at,public_metrics";
  for (let n = 0; n < 40; n++) {
    const qs = new URLSearchParams({
      max_results: "100",
      "tweet.fields": fields,
      exclude: "retweets",
      start_time: startIso,
    });
    if (token) qs.set("pagination_token", token);
    const path = `users/${userId}/tweets?${qs.toString()}`;
    const data = await withRetries(() => twitterFetch(path, bearer), { retries: 4, baseMs: 2000 });
    const batch = data.data || [];
    for (const tw of batch) {
      const m = tw.public_metrics || {};
      tweets.push({
        id: tw.id,
        tweet_text: tw.text || "",
        created_at: tw.created_at,
        likes: m.like_count ?? 0,
        retweets: m.retweet_count ?? 0,
        replies: m.reply_count ?? 0,
      });
    }
    token = data.meta?.next_token;
    if (!token || tweets.length >= 3200) break;
  }
  return tweets.slice(0, 3200);
}

async function fetchViaApify(handle, startIso) {
  const token = process.env.APIFY_API_TOKEN;
  const actor = process.env.APIFY_TWITTER_ACTOR_ID;
  if (!token) throw new Error("Apify token not configured");
  if (!actor) throw new Error("Set APIFY_TWITTER_ACTOR_ID");
  const input = {
    searchTerms: [`from:${handle.replace(/^@/, "")}`],
    maxTweets: 3200,
    since: startIso.slice(0, 10),
  };
  const runRes = await fetch(
    `https://api.apify.com/v2/acts/${actor}/runs?token=${token}&waitForFinish=120`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(input),
    }
  );
  if (!runRes.ok) {
    const t = await runRes.text();
    throw new Error(`Apify run failed: ${runRes.status} ${t.slice(0, 300)}`);
  }
  const run = await runRes.json();
  const ds = run.data?.defaultDatasetId;
  if (!ds) throw new Error("Apify: no dataset id");
  const itemsRes = await fetch(
    `https://api.apify.com/v2/datasets/${ds}/items?token=${token}&clean=true&format=json`
  );
  if (!itemsRes.ok) throw new Error("Apify dataset fetch failed");
  const items = await itemsRes.json();
  const tweets = [];
  for (const it of items) {
    const text = it.text || it.full_text || "";
    if (!text) continue;
    tweets.push({
      id: String(it.id || it.tweetId || Math.random()),
      tweet_text: text,
      created_at: it.createdAt || it.created_at || new Date().toISOString(),
      likes: it.likeCount ?? it.like_count ?? 0,
      retweets: it.retweetCount ?? it.retweet_count ?? 0,
      replies: it.replyCount ?? it.reply_count ?? 0,
    });
  }
  tweets.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  return tweets.slice(0, 3200);
}

function dedupeTweets(tweets) {
  const seen = new Set();
  const out = [];
  for (const t of tweets) {
    const key = `${(t.created_at || "").slice(0, 10)}|${(t.tweet_text || "").slice(0, 140).toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(t);
  }
  return out.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
}

export async function runScraperAgent(handle, emit, opts = {}) {
  const normalized = handle.replace(/^@/, "").toLowerCase();
  const start = new Date(Date.now() - THREE_YEARS_MS).toISOString();
  const forceRescrape =
    Boolean(opts.forceRescrape) || process.env.FORCE_RESCRAPE === "true";
  let tweets = [];
  let profile = null;
  let source = "mock";

  if (process.env.USE_MOCK_DATA === "true") {
    emit({ stage: "scraper", message: "Using mock tweet corpus (USE_MOCK_DATA=true)…", progress: 0.06 });
    profile = mockProfile(normalized);
    tweets = mockTweets(normalized);
    source = "mock";
  } else if (!forceRescrape) {
    const meta = getCorpusMeta(normalized);
    const cached = getTweetsForHandle(normalized);
    const span = corpusSpanDays(cached[0]?.created_at, cached[cached.length - 1]?.created_at);
    const trusted = corpusSourceTrusted(meta?.source);
    if (trusted && cached.length >= MIN_TWEETS_OK && span >= MIN_CORPUS_SPAN_DAYS) {
      let twProfile = await lookupTwitterProfileByUsername(normalized);
      if (!twProfile) {
        const prev = getAnalysis(normalized);
        if (prev?.profile?.username) twProfile = prev.profile;
      }
      if (!twProfile) twProfile = mockProfile(normalized);
      const sorted = [...cached].sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
      emit({
        stage: "scraper",
        message: `Loaded ${sorted.length} posts from saved corpus (database • ${meta.source}).`,
        detail: `Span ≈ ${Math.round(span)} days. Set FORCE_RESCRAPE=true to fetch fresh posts.`,
        progress: 0.24,
        done: true,
      });
      return {
        handle: normalized,
        profile: {
          id: twProfile.id || "cached",
          name: twProfile.name,
          username: twProfile.username || normalized,
          description: twProfile.description || "",
          profile_image_url: twProfile.profile_image_url || "",
        },
        tweets: sorted,
        dateRange: {
          start: sorted[0]?.created_at || start,
          end: sorted[sorted.length - 1]?.created_at || new Date().toISOString(),
        },
        scrapeMeta: {
          source: "database_cache",
          underlying: meta.source,
          tweet_count: sorted.length,
        },
      };
    }
  }

  if (process.env.USE_MOCK_DATA !== "true") {
    /** Step 1 — archives */
    emit({ stage: "scraper", message: "Searching for tweet archives online…", progress: 0.04 });
    const hasSearch = Boolean(process.env.SERPER_API_KEY || process.env.BRAVE_API_KEY);
    if (!hasSearch) {
      emit({
        stage: "scraper",
        message: "Searching for tweet archives online…",
        detail: "No SERPER_API_KEY or BRAVE_API_KEY — skipping automated archive discovery.",
        progress: 0.06,
      });
    }
    const archiveRows = hasSearch
      ? await discoverAndIngestArchives(normalized, searchWeb, (d) => emit({ stage: "scraper", ...d }))
      : [];
    if (archiveRows.length >= MIN_TWEETS_OK) {
      tweets = archiveRows;
      source = "archive_web";
      emit({
        stage: "scraper",
        message: `Loaded ${tweets.length} posts from an online archive or bulk export.`,
        progress: 0.18,
      });
    }

    /** Step 2 — Twitter API (+ Apify) */
    const bearer = process.env.TWITTER_BEARER_TOKEN;
    if (tweets.length < MIN_TWEETS_OK && bearer) {
      emit({ stage: "scraper", message: "Fetching via API…", progress: 0.1 });
      try {
        profile = await fetchUserByUsername(normalized, bearer);
        tweets = await fetchAllTweets(profile.id, bearer, start);
        source = "twitter_api";
        emit({
          stage: "scraper",
          message: `Retrieved ${tweets.length} tweets via Twitter/X API v2.`,
          progress: 0.16,
        });
      } catch (e) {
        emit({
          stage: "scraper",
          message: "Fetching via API…",
          detail: String(e.message || e),
          progress: 0.12,
        });
        try {
          tweets = await fetchViaApify(normalized, start);
          source = "apify";
          profile = await fetchUserByUsername(normalized, bearer).catch(() => mockProfile(normalized));
          emit({
            stage: "scraper",
            message: `Retrieved ${tweets.length} tweets via Apify fallback.`,
            progress: 0.16,
          });
        } catch (e2) {
          emit({
            stage: "scraper",
            message: "Fetching via API…",
            detail: `Apify failed: ${String(e2.message || e2)}`,
            progress: 0.12,
          });
        }
      }
    } else if (tweets.length < MIN_TWEETS_OK && !bearer) {
      emit({
        stage: "scraper",
        message: "Fetching via API…",
        detail: "No TWITTER_BEARER_TOKEN configured.",
        progress: 0.1,
      });
    }

    /** Step 3 — screenshots + Claude Vision */
    if (tweets.length < MIN_TWEETS_OK) {
      emit({ stage: "scraper", message: "Capturing screenshots…", progress: 0.18 });
      const xUrl = `https://x.com/${normalized}`;
      try {
        const visionTweets = await runScreenshotVisionScrape(normalized, {
          profileUrl: xUrl,
          emit: (d) =>
            emit({
              stage: "scraper",
              message: "Capturing screenshots…",
              detail: d.visionError || `Vision pass ${d.shot}/${d.total}`,
              progress: 0.18 + 0.04 * ((d.shot || 0) / Math.max(d.total || 1, 1)),
            }),
        });
        if (visionTweets.length) {
          tweets = dedupeTweets([...tweets, ...visionTweets]);
          source = source === "mock" ? "vision" : `${source}+vision`;
        }
        emit({
          stage: "scraper",
          message: `Vision extraction yielded ${visionTweets.length} post snippets (deduped total ${tweets.length}).`,
          progress: 0.22,
        });
      } catch (e) {
        emit({
          stage: "scraper",
          message: "Capturing screenshots…",
          detail: String(e.message || e),
          progress: 0.2,
        });
      }
    }

    if (!profile) {
      profile = bearer
        ? await fetchUserByUsername(normalized, bearer).catch(() => mockProfile(normalized))
        : mockProfile(normalized);
    }

    const hasRealTweets = tweets.length > 0 && source !== "mock" && source !== "mock_fallback";
    if (tweets.length < MIN_TWEETS_OK && !hasRealTweets) {
      emit({
        stage: "scraper",
        message: "Insufficient data after all steps — using mock corpus so downstream agents can run.",
        progress: 0.22,
      });
      tweets = mockTweets(normalized, 200);
      source = "mock_fallback";
      profile = profile || mockProfile(normalized);
    } else if (tweets.length < MIN_TWEETS_OK && hasRealTweets) {
      emit({
        stage: "scraper",
        message: `Proceeding with limited real corpus (${tweets.length} posts).`,
        detail: "Results may be lower-confidence, but evidence remains real (not mock fallback).",
        progress: 0.22,
      });
    }
  }

  tweets = dedupeTweets(tweets);

  clearTweetsForHandle(normalized);
  for (const t of tweets) {
    insertTweet({
      id: t.id,
      handle: normalized,
      tweet_text: t.tweet_text,
      created_at: t.created_at,
      likes: t.likes,
      retweets: t.retweets,
      replies: t.replies,
    });
  }

  const sorted = [...tweets].sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  saveCorpusMeta(normalized, {
    source,
    tweet_count: sorted.length,
    oldest_at: sorted[0]?.created_at ?? null,
    newest_at: sorted[sorted.length - 1]?.created_at ?? null,
  });
  emit({
    stage: "scraper",
    message: `Stored ${sorted.length} tweets in database (${source}).`,
    progress: 0.24,
    done: true,
  });

  return {
    handle: normalized,
    profile: {
      id: profile.id,
      name: profile.name,
      username: profile.username,
      description: profile.description || "",
      profile_image_url: profile.profile_image_url || "",
    },
    tweets: sorted,
    dateRange: {
      start: sorted[0]?.created_at || start,
      end: sorted[sorted.length - 1]?.created_at || new Date().toISOString(),
    },
    scrapeMeta: { source, tweet_count: sorted.length },
  };
}
