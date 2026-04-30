import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { ReactNode } from "react";
import type { EvidenceTweet } from "@/data/mockData";

interface Props {
  evidence: EvidenceTweet[];
  children: ReactNode;
}

function EvidenceList({ evidence }: { evidence: EvidenceTweet[] }) {
  if (!evidence.length) {
    return (
      <div className="border border-primary/10 rounded p-4 text-sm text-muted-foreground">
        No evidence posts found for this section yet.
      </div>
    );
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {evidence.map((tweet) => (
        <article key={tweet.id} className="border border-primary/15 rounded p-3 bg-card/40 flex flex-col">
          <p className="text-sm text-muted-foreground leading-relaxed">{tweet.text}</p>
          <div className="mt-3 font-mono text-[10px] text-muted-foreground/70">
            <span>{tweet.createdAt ? new Date(tweet.createdAt).toLocaleDateString() : "Unknown date"}</span>
          </div>
          <div className="mt-2 flex items-center justify-between font-mono text-[10px] text-muted-foreground/70">
            {tweet.url ? (
              <a
                href={tweet.url}
                target="_blank"
                rel="noreferrer"
                className="inline-block tracking-wider text-primary hover:underline"
              >
                OPEN POST
              </a>
            ) : (
              <span />
            )}
            <span className="whitespace-nowrap">
              RT {tweet.retweets} | Likes {tweet.likes}
            </span>
          </div>
        </article>
      ))}
    </div>
  );
}

const SectionEvidenceTabs = ({ evidence, children }: Props) => {
  return (
    <Tabs defaultValue="conclusion" className="w-full">
      <TabsList className="mb-4">
        <TabsTrigger value="conclusion">Conclusion</TabsTrigger>
        <TabsTrigger value="evidence">View Evidence</TabsTrigger>
      </TabsList>
      <TabsContent value="conclusion">{children}</TabsContent>
      <TabsContent value="evidence">
        <EvidenceList evidence={evidence} />
        <p className="mt-2 font-mono text-[10px] text-muted-foreground/50">
          Evidence is sampled supporting posts for this section. Open Post shows the original tweet when URL is available.
        </p>
      </TabsContent>
    </Tabs>
  );
};

export default SectionEvidenceTabs;
