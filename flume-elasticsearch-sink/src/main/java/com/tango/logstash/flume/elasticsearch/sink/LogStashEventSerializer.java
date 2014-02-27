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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;

public class LogStashEventSerializer implements ElasticSearchEventSerializer {

    private final static String FIELDS_PREFIX = "@fields";
    
    private final static String SRC_PATH_HEADER = "src_path";
    private final static String HOST_HEADER = "host";
    private final static String TYPE_HEADER = "type";
    private final static String TAGS_HEADER = "tags";
    private final static String SOURCE_HEADER = "source";
    private final static String MESSAGE_HEADER = "message";
    private final static String AT_MESSAGE_HEADER = "@message";
    private final static String TIMESTAMP_HEADER = "timestamp";
    private final static String AT_TIMESTAMP_HEADER = "@timestamp";
    private final static String AT_SOURCE_HEADER = "@source";
    private final static String AT_TYPE_HEADER = "@type";
    private final static String AT_TAGS_HEADER = "@tags";
    private final static String AT_VERSION_HEADER = "@version";
    private final static String AT_SOURCE_HOST_HEADER = "@source_host";
    private final static String AT_SOURCE_PATH_HEADER = "@source_path";

    private final Set<String> blackListedExtraFields = new HashSet<String>();

    @Override
    public XContentBuilder getContentBuilder(Event event) throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        appendBody(builder, event);
        appendHeaders(builder, event);
        return builder;
    }

    private void appendBody(XContentBuilder builder, Event event) throws IOException, UnsupportedEncodingException {
        Map<String, String> headers = Maps.newHashMap(event.getHeaders());

        byte[] body = event.getBody();
        if ((body == null || body.length == 0) && headers != null && headers.containsKey(MESSAGE_HEADER)) {
            body = headers.get(MESSAGE_HEADER).getBytes();
        }

        ContentBuilderUtil.appendField(builder, AT_MESSAGE_HEADER, body);
    }

    private void extractHeader(XContentBuilder builder, Map<String, String> headers, String fieldName,
            String alternativeFieldName) throws IOException {

        String alternativeFieldValue = headers.get(alternativeFieldName);
        String fieldValue = headers.get(fieldName);

        byte[] finalValue = null;
        if (StringUtils.isNotBlank(fieldValue)) {
            finalValue = fieldValue.getBytes(charset);
        } else if (StringUtils.isNotBlank(alternativeFieldValue)) {
            finalValue = alternativeFieldValue.getBytes(charset);
        }

        if (finalValue != null) {
            ContentBuilderUtil.appendField(builder, fieldName, finalValue);
        }
    }

    private void appendHeaders(XContentBuilder builder, Event event) throws IOException {
        Map<String, String> headers = Maps.newHashMap(event.getHeaders());

        String timestamp = headers.get(TIMESTAMP_HEADER);
        String atTimestamp = headers.get(AT_TIMESTAMP_HEADER);
        if (StringUtils.isNotBlank(atTimestamp)) {
            builder.field(AT_TIMESTAMP_HEADER, atTimestamp);
        } else if (StringUtils.isNotBlank(timestamp)) {
            long timestampMs = Long.parseLong(timestamp);
            builder.field(AT_TIMESTAMP_HEADER, new Date(timestampMs));
        }

        extractHeader(builder, headers, AT_SOURCE_HEADER, SOURCE_HEADER);
        extractHeader(builder, headers, AT_TYPE_HEADER, TYPE_HEADER);
        extractHeader(builder, headers, AT_TAGS_HEADER, TAGS_HEADER);
        extractHeader(builder, headers, AT_SOURCE_HOST_HEADER, HOST_HEADER);
        extractHeader(builder, headers, AT_SOURCE_PATH_HEADER, SRC_PATH_HEADER);

        builder.startObject(FIELDS_PREFIX);
        for (String key : headers.keySet()) {
            if (blackListedExtraFields.contains(key) == false) {
                byte[] val = headers.get(key).getBytes(charset);
                ContentBuilderUtil.appendField(builder, key, val);
            }
        }
        builder.endObject();
    }
    @Override
    public void configure(Context context) {
        // TODO Make black listed fields configurable
        blackListedExtraFields.add(AT_MESSAGE_HEADER);
        blackListedExtraFields.add(AT_SOURCE_HEADER);
        blackListedExtraFields.add(AT_SOURCE_HOST_HEADER);
        blackListedExtraFields.add(AT_SOURCE_PATH_HEADER);
        blackListedExtraFields.add(AT_TIMESTAMP_HEADER);
        blackListedExtraFields.add(AT_TYPE_HEADER);
        blackListedExtraFields.add(AT_TAGS_HEADER);
        blackListedExtraFields.add(AT_VERSION_HEADER);
        blackListedExtraFields.add(MESSAGE_HEADER);
        blackListedExtraFields.add(TIMESTAMP_HEADER);
        blackListedExtraFields.add(SOURCE_HEADER);
        blackListedExtraFields.add(TYPE_HEADER);
        blackListedExtraFields.add(HOST_HEADER);
        blackListedExtraFields.add(SRC_PATH_HEADER);
        blackListedExtraFields.add(TAGS_HEADER);
    }

    @Override
    public void configure(ComponentConfiguration conf) {
        // NO-OP...
    }

}
