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

import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.EventSerializerIndexRequestBuilderFactory;

public class LogstashEventSerializerIndexRequestBuilderFactory extends EventSerializerIndexRequestBuilderFactory {

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
}
