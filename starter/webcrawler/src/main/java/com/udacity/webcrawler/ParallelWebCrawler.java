package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;
import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;



/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;
  private final PageParserFactory parserFactory;


  @Inject
  ParallelWebCrawler(
          Clock clock,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount, @MaxDepth int maxDepth, @IgnoredUrls List<Pattern> ignoredUrls,
          PageParserFactory parserFactory) {
    this.clock = clock;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
    this.parserFactory = parserFactory;

  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadline = clock.instant().plus(timeout);
    ConcurrentMap<String, Integer> counts = new ConcurrentHashMap<>();
    ConcurrentSkipListSet<String> visitedUrls = new ConcurrentSkipListSet<>();

    for (String url : startingUrls) {
      CustomParallelCrawlerInternal customParallelCrawler = new CustomParallelCrawlerInternal(url, deadline, maxDepth, counts, visitedUrls, clock, parserFactory, ignoredUrls);
      pool.invoke(customParallelCrawler);
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setWordCounts(counts)
              .setUrlsVisited(visitedUrls.size())
              .build();
    }
    return new CrawlResult.Builder()
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .setUrlsVisited(visitedUrls.size())
            .build();
  }


  public class CustomParallelCrawlerInternal extends RecursiveAction {
    private String url;
    private Instant deadline;
    private int maxDepth;
    private ConcurrentMap<String, Integer> counts;
    private ConcurrentSkipListSet<String> visitedUrls;
    private Clock clock;
    private PageParserFactory parserFactory;
    private List<Pattern> ignoredUrls;


    public CustomParallelCrawlerInternal(String url, Instant deadline, int maxDepth, ConcurrentMap<String, Integer> counts, ConcurrentSkipListSet<String> visitedUrls, Clock clock,
                                         PageParserFactory parserFactory, List<Pattern> ignoredUrls) {
      this.url = url;
      this.deadline = deadline;
      this.maxDepth = maxDepth;
      this.counts = counts;
      this.visitedUrls = visitedUrls;
      this.clock = clock;
      this.parserFactory = parserFactory;
      this.ignoredUrls = ignoredUrls;
    }

    @Override
    protected void compute() {
      if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
        return;
      }
      for (Pattern pattern : ignoredUrls) {
        if (pattern.matcher(url).matches()) {
          return;
        }
      }
      synchronized (visitedUrls) {
        if (visitedUrls.contains(url)) {
          return;
        }
        visitedUrls.add(url);
      }

        PageParser.Result result = parserFactory.get(url).parse();
        for (ConcurrentMap.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
        counts.compute(e.getKey(), (k, v) -> (v == null) ? e.getValue() : e.getValue() + v);
       }

        List<CustomParallelCrawlerInternal> subtasks = new ArrayList<>();
        for (String link : result.getLinks()) {
          CustomParallelCrawlerInternal customParallelCrawler = new CustomParallelCrawlerInternal(link, deadline, maxDepth - 1, counts, visitedUrls, clock, parserFactory, ignoredUrls);
          subtasks.add(customParallelCrawler);
        }
        invokeAll(subtasks);
        return;
      }

    }

    @Override
    public int getMaxParallelism() {
      return Runtime.getRuntime().availableProcessors();
    }

}
