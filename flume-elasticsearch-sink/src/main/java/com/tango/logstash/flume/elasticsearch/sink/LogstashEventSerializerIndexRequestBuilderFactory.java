/**
 *  Copyright 2014 TangoMe Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.tango.logstash.flume.elasticsearch.sink;

import java.io.IOException;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.EventSerializerIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.io.BytesStream;

public class LogstashEventSerializerIndexRequestBuilderFactory extends EventSerializerIndexRequestBuilderFactory {

    private static final String DOCUMENT_ID = "_id";

    private final static String PARAM_INDEX_FORMAT = "indexFormat";

    // Default value
    private FastDateFormat indexFormater = ElasticSearchIndexRequestBuilderFactory.df;

    public LogstashEventSerializerIndexRequestBuilderFactory() {
        this(new LogStashEventSerializer());
    }

    public LogstashEventSerializerIndexRequestBuilderFactory(ElasticSearchEventSerializer serializer) {
        super(serializer);
    }
    @Override
    public void configure(Context context) {
        super.configure(context);

        // Ex: "yyyy-MM-dd"
        String indexFormatStr = context.getString(PARAM_INDEX_FORMAT);
        if (StringUtils.isNotBlank(indexFormatStr)) {
            indexFormater = FastDateFormat.getInstance(indexFormatStr, TimeZone.getTimeZone("Etc/UTC"));
        }
    }

    @Override
    protected String getIndexName(String indexPrefix, long timestamp) {
        return new StringBuilder(indexPrefix).append('-').append(indexFormater.format(timestamp)).toString();
    }

    private void setId(IndexRequestBuilder indexRequest, Event event) {
        final Map<String, String> headers = event.getHeaders();

        if (indexRequest != null && MapUtils.isNotEmpty(headers)) {
            if (headers.containsKey(DOCUMENT_ID)) {
                String idValue = headers.get(DOCUMENT_ID);
                indexRequest.setId(idValue);
            }
        }
    }

    @Override
    protected void prepareIndexRequest(IndexRequestBuilder indexRequest, String indexName, String indexType, Event event)
            throws IOException {
        BytesStream contentBuilder = serializer.getContentBuilder(event);
        indexRequest.setIndex(indexName).setType(indexType).setSource(contentBuilder.bytes());
        setId(indexRequest, event);
    }

}
