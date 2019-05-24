package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.sampler.DiversifiedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

/**
 * This spout is based on Aggregation Spout but adds the possibility to set the
 * distribution of top level domains. Distribution is controlled by
 * es.status.staticTld.distribution configuration parameter. (eg.:
 * de:10;at:10;ch:80)
 * 
 * @author maik.strewe
 *
 */
public class StaticTldBalancingSpout extends AbstractSpout implements ActionListener<SearchResponse> {

	private static final Logger LOG = LoggerFactory.getLogger(StaticTldBalancingSpout.class);

	private static final String ESStatusSampleParamName = "es.status.sample";
	private static final String ESMostRecentDateIncreaseParamName = "es.status.recentDate.increase";
	private static final String ESMostRecentDateMinGapParamName = "es.status.recentDate.min.gap";
	private static final String StaticTldBalancingSpoutDistributionParamName = "es.status.staticTld.distribution";

	private boolean sample = false;

	private int recentDateIncrease = -1;
	private int recentDateMinGap = -1;

	/**
	 * Holds latest URLs from ES query before they get into the Buffer. The
	 * balancing logic will be done on this Buffer.
	 */
	private HashMap<String, Map<String, Object>> preBuffer;

	/**
	 * Map holds configured Distribution of top level domains.
	 */
	private HashMap<String, Integer> distribution;

	/**
	 * Map holds the current distribution state. This will set to distribution when
	 * all counter are 0;
	 */
	private HashMap<String, Integer> distributionCounter;

	/**
	 * Memorizes sum of URLs in distribution counter. This is needed to check if for
	 * sum reason the distribution can not be fulfilled. Then we have to reset the
	 * distribution counter, because we have no chance to fulfill the configured
	 * distribution.
	 */
	private int oldDistributionSum;

	private MultiCountMetric statsMetric;

	@Override
	public void open(Map stormConf, TopologyContext context, SpoutOutputCollector collector) {
		sample = ConfUtils.getBoolean(stormConf, ESStatusSampleParamName, sample);
		recentDateIncrease = ConfUtils.getInt(stormConf, ESMostRecentDateIncreaseParamName, recentDateIncrease);
		recentDateMinGap = ConfUtils.getInt(stormConf, ESMostRecentDateMinGapParamName, recentDateMinGap);
		statsMetric = context.registerMetric("tld_balance_stats", new MultiCountMetric(), 30);

		// Read configuration about tld distribution and setup the counter

		String distributionString = ConfUtils.getString(stormConf, StaticTldBalancingSpoutDistributionParamName);
		if (distributionString == null || distributionString.trim().isEmpty())
			throw new RuntimeException("No distribution of Tld's found. StaticTldBalancingSpout must have "
					+ StaticTldBalancingSpoutDistributionParamName + " configured. (E.g. de:10;at:10;ch:80");
		String[] distributions = distributionString.split(";");
		distribution = new HashMap<>();
		distributionCounter = new HashMap<>();
		int sum = 0;
		int counter = 0;
		try {
			for (String item : distributions) {
				Integer i = Integer.parseInt(item.split(":")[1]);
				distribution.put(item.split(":")[0], i);
				distributionCounter.put(item.split(":")[0], i);
				sum += i.intValue();
				counter++;
			}
			if (sum != 100)
				throw new Exception();
		} catch (Exception e) {
			throw new RuntimeException(StaticTldBalancingSpoutDistributionParamName
					+ " seems to be misconfigured. Format:  de:10;at:10;ch:80\r\nSum must be 100");
		}

		super.open(stormConf, context, collector);
	}

	@Override
	protected void populateBuffer() {

		if (queryDate == null) {
			queryDate = new Date();
			lastTimeResetToNOW = Instant.now();
		}

		String formattedQueryDate = ISODateTimeFormat.dateTimeNoMillis().print(queryDate.getTime());

		LOG.info("{} Populating buffer with nextFetchDate <= {}", logIdprefix, formattedQueryDate);

		BoolQueryBuilder queryBuilder = boolQuery()
				.filter(QueryBuilders.rangeQuery("nextFetchDate").lte(formattedQueryDate));

		if (filterQuery != null) {
			queryBuilder = queryBuilder.filter(QueryBuilders.queryStringQuery(filterQuery));
		}

		SearchRequest request = new SearchRequest(indexName);

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		sourceBuilder.query(queryBuilder);
		sourceBuilder.from(0);
		sourceBuilder.size(0);
		sourceBuilder.explain(false);
		sourceBuilder.trackTotalHits(false);

		TermsAggregationBuilder tldAggregation = AggregationBuilders.terms("tld").field("metadata.tld");

		TermsAggregationBuilder aggregations = AggregationBuilders.terms("partition").field(partitionField)
				.size(maxBucketNum);

		org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder tophits = AggregationBuilders
				.topHits("docs").size(maxURLsPerBucket).explain(false);
		// sort within a bucket
		if (StringUtils.isNotBlank(bucketSortField)) {
			FieldSortBuilder sorter = SortBuilders.fieldSort(bucketSortField).order(SortOrder.ASC);
			tophits.sort(sorter);
		}

		aggregations.subAggregation(tophits);

		// sort between buckets
		if (StringUtils.isNotBlank(totalSortField)) {
			org.elasticsearch.search.aggregations.metrics.MinAggregationBuilder minBuilder = AggregationBuilders
					.min("top_hit").field(totalSortField);
			aggregations.subAggregation(minBuilder);
			aggregations.order(BucketOrder.aggregation("top_hit", true));
		}

		tldAggregation.subAggregation(aggregations);

		if (sample) {
			DiversifiedAggregationBuilder sab = new DiversifiedAggregationBuilder("sample");
			sab.field(partitionField).maxDocsPerValue(maxURLsPerBucket);
			sab.shardSize(maxURLsPerBucket * maxBucketNum);
			sab.subAggregation(tldAggregation);
			sourceBuilder.aggregation(sab);
		} else {
			sourceBuilder.aggregation(tldAggregation);
		}

		request.source(sourceBuilder);

		// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-preference.html
		// _shards:2,3
		if (shardID != -1) {
			request.preference("_shards:" + shardID);
		}

		// dump query to log
		LOG.debug("{} ES query {}", logIdprefix, request.toString());

		isInQuery.set(true);
		client.searchAsync(request, RequestOptions.DEFAULT, this);
	}

	@Override
	public void onFailure(Exception arg0) {
		LOG.error("Exception with ES query", arg0);
		markQueryReceivedNow();
	}

	@Override
	public void onResponse(SearchResponse response) {
		long timeTaken = System.currentTimeMillis() - getTimeLastQuerySent();

		Aggregations aggregs = response.getAggregations();

		if (aggregs == null) {
			markQueryReceivedNow();
			return;
		}

		SingleBucketAggregation sample = aggregs.get("sample");
		if (sample != null) {
			aggregs = sample.getAggregations();
		}

		Terms tldAgg = aggregs.get("tld");
		Iterator<Terms.Bucket> tldIterator = (Iterator<Bucket>) tldAgg.getBuckets().iterator();

		int numhits = 0;
		int numBuckets = 0;
		int alreadyprocessed = 0;

		Date mostRecentDateFound = null;

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

		synchronized (this.preBuffer) {
			while (tldIterator.hasNext()) {

				Bucket tldBucket = tldIterator.next();
				String currentTld = (String) tldBucket.getKey();
				if (distribution.keySet().contains(currentTld) == false) {
					LOG.info("Tld " + currentTld + " is not configured in distribution. Skip.");
					continue;
				}

				// For each entry

				Terms agg = tldBucket.getAggregations().get("partition");
				Iterator<Terms.Bucket> iterator = (Iterator<Bucket>) agg.getBuckets().iterator();
				while (iterator.hasNext()) {
					Terms.Bucket entry = iterator.next();
					String key = (String) entry.getKey(); // bucket key
					long docCount = entry.getDocCount(); // Doc count

					int hitsForThisBucket = 0;

					// filter results so that we don't include URLs we are already
					// being processed
					TopHits topHits = entry.getAggregations().get("docs");
					for (SearchHit hit : topHits.getHits().getHits()) {
						hitsForThisBucket++;

						Map<String, Object> keyValues = hit.getSourceAsMap();
						String url = (String) keyValues.get("url");
						LOG.debug("{} -> id [{}], _source [{}]", logIdprefix, hit.getId(), hit.getSourceAsString());

						// consider only the first document of the last bucket
						// for optimising the nextFetchDate
						if (hitsForThisBucket == 1 && !iterator.hasNext()) {
							String strDate = (String) keyValues.get("nextFetchDate");
							try {
								mostRecentDateFound = formatter.parse(strDate);
							} catch (ParseException e) {
								throw new RuntimeException("can't parse date :" + strDate);
							}
						}

						// is already being processed or in buffer - skip it!
						if (beingProcessed.containsKey(url) || in_buffer.contains(url)) {
							LOG.debug("{} -> already processed or in buffer : {}", url);
							alreadyprocessed++;
							continue;
						}

						LOG.debug("{} -> added to preBuffer : {}", url);

						addToPreBuffer(tldBucket.getKey().toString(), url, keyValues);

					}

					if (hitsForThisBucket > 0)
						numBuckets++;

					numhits += hitsForThisBucket;

					LOG.debug("{} key [{}], hits[{}], doc_count [{}]", logIdprefix, key, hitsForThisBucket, docCount,
							alreadyprocessed);
				}
			}

			populatePreBuffer();

			// Shuffle the URLs so that we don't get blocks of URLs from the
			// same
			// host or domain
			// Collections.shuffle((List) buffer);
		}

		LOG.info(
				"{} ES query returned {} hits from {} buckets in {} msec with {} already being processed. Took {} msec per doc on average.",
				logIdprefix, numhits, numBuckets, timeTaken, alreadyprocessed, ((float) timeTaken / numhits));

		queryTimes.addMeasurement(timeTaken);
		eventCounter.scope("already_being_processed").incrBy(alreadyprocessed);
		eventCounter.scope("ES_queries").incrBy(1);
		eventCounter.scope("ES_docs").incrBy(numhits);

		// optimise the nextFetchDate by getting the most recent value
		// returned in the query and add to it, unless the previous value is
		// within n mins in which case we'll keep it
		if (mostRecentDateFound != null && recentDateIncrease >= 0) {
			Calendar potentialNewDate = Calendar.getInstance();
			potentialNewDate.setTime(mostRecentDateFound);
			potentialNewDate.add(Calendar.MINUTE, recentDateIncrease);
			Date oldDate = null;
			// check boundaries
			if (this.recentDateMinGap > 0) {
				Calendar low = Calendar.getInstance();
				low.setTime(queryDate);
				low.add(Calendar.MINUTE, -recentDateMinGap);
				Calendar high = Calendar.getInstance();
				high.setTime(queryDate);
				high.add(Calendar.MINUTE, recentDateMinGap);
				if (high.before(potentialNewDate) || low.after(potentialNewDate)) {
					oldDate = queryDate;
				}
			} else {
				oldDate = queryDate;
			}
			if (oldDate != null) {
				queryDate = potentialNewDate.getTime();
				LOG.info("{} queryDate changed from {} to {} based on mostRecentDateFound {}", logIdprefix, oldDate,
						queryDate, mostRecentDateFound);
			} else {
				LOG.info("{} queryDate kept at {} based on mostRecentDateFound {}", logIdprefix, queryDate,
						mostRecentDateFound);
			}
		}

		// reset the value for next fetch date if the previous one is too old
		if (resetFetchDateAfterNSecs != -1) {
			Instant changeNeededOn = Instant
					.ofEpochMilli(lastTimeResetToNOW.toEpochMilli() + (resetFetchDateAfterNSecs * 1000));
			if (Instant.now().isAfter(changeNeededOn)) {
				LOG.info("{} queryDate set to null based on resetFetchDateAfterNSecs {}", logIdprefix,
						resetFetchDateAfterNSecs);
				queryDate = null;
			}
		}

		// change the date if we don't get any results at all
		if (numBuckets == 0) {
			queryDate = null;
		}

		// remove lock
		markQueryReceivedNow();
	}

	private void populatePreBuffer() {
		synchronized (buffer) {

			Map<String, HashMap<String, Object>> nextChunk = getNextURL();
			long start = java.lang.System.currentTimeMillis();
			int counter = 0;
			while (nextChunk != null) {
				for (Entry<String, HashMap<String, Object>> values : nextChunk.entrySet()) {
					Metadata metadata = fromKeyValues((Map<String, Object>) values.getValue());
					statsMetric.scope("buffer_added_" + metadata.getFirstValue("tld")).incr();
					buffer.add(new Values(values.getKey(), metadata));
					counter++;
				}
				nextChunk = getNextURL();
			}
			long milliesTaken = java.lang.System.currentTimeMillis() - start;
			LOG.info("populatePreBuffer took " + milliesTaken + " ms. (For " + counter + " URLs)");
		}
	}

	private Map<String, HashMap<String, Object>> getNextURL() {
		Map<String, HashMap<String, Object>> retval = new HashMap<String, HashMap<String, Object>>();
		for (String tld : distribution.keySet()) {
			LOG.debug("Handle TLD: " + tld);
			Map<String, Object> urlsOfTld = preBuffer.get(tld);
			if (urlsOfTld == null || urlsOfTld.size() == 0) {
				continue;
			}
			if (distributionCounter.get(tld) == null) {
				continue;
			}
			if (distributionCounter.get(tld) <= 0) {
				continue;
			}
			Iterator<String> keySet = urlsOfTld.keySet().iterator();

			String currentKey = keySet.next();
			if (LOG.isTraceEnabled())
				LOG.trace("Next url for buffer" + currentKey + ".");
			retval.put(currentKey, (HashMap<String, Object>) urlsOfTld.get(currentKey));
			distributionCounter.put(tld, (distributionCounter.get(tld) - 1));
			urlsOfTld.remove(currentKey);

			// remove current url from pre buffer
			preBuffer.put(tld, urlsOfTld);
			break;
		}

		checkForDistributionCounterReset();

		if (retval.size() == 0)
			retval = null;
		return retval;
	}

	private void addToPreBuffer(String key, String url, Map<String, Object> keyValues) {
		Map<String, Object> tldMap = preBuffer.get(key);
		if (tldMap == null) {
			preBuffer.put(key, new HashMap<String, Object>());
			return;
		}
		if (tldMap.containsKey(url))
			return;
		else
			tldMap.put(url, keyValues);

	}

	/**
	 * Reset the distribution counter if count is 0 (current distribution
	 * fulfilled). Or if no URL could be added to Buffer (current distribution not
	 * fulfilled).
	 */
	private void checkForDistributionCounterReset() {
		int currentSum = 0;
		for (String tld : distribution.keySet()) {
			if (distributionCounter.get(tld) != null)
				currentSum += distributionCounter.get(tld);
		}
		if (currentSum == oldDistributionSum || currentSum == 0)
			resetDistributionCounter();
		LOG.debug("Districounter not resetted. (" + currentSum + ")");

		oldDistributionSum = currentSum;
	}

	private void resetDistributionCounter() {
		LOG.debug("Districounter is resetted.");
		distributionCounter = new HashMap<>(distribution);

	}

}
