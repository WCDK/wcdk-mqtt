package com.wcdk.mqtt.influx;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.BucketRetentionRules;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import org.springframework.util.StringUtils;

import java.lang.reflect.Field;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @auther WCDK
 * @date 2026/7/8
 * @version 1.0
 **/
public class InfluxDbUtil {

    private final InfluxProperties properties;

    private final WriteApiBlocking writeApiBlocking;

    private final QueryApi queryApi;

    private final DeleteApi deleteApi;

    private final BucketsApi bucketsApi;

    private final OrganizationsApi organizationsApi;

    public InfluxDbUtil(InfluxProperties properties,
                        WriteApiBlocking writeApiBlocking,
                        QueryApi queryApi,
                        DeleteApi deleteApi,
                        BucketsApi bucketsApi,
                        OrganizationsApi organizationsApi) {
        this.properties = properties;
        this.writeApiBlocking = writeApiBlocking;
        this.queryApi = queryApi;
        this.deleteApi = deleteApi;
        this.bucketsApi = bucketsApi;
        this.organizationsApi = organizationsApi;
    }

    public Bucket createBucketIfMissing() {
        return createBucketIfMissing(properties.getBucket(), properties.getOrg());
    }

    public Organization createOrganizationIfMissing() {
        return createOrganizationIfMissing(properties.getOrg());
    }

    public Organization createOrganizationIfMissing(String organizationName) {
        Organization organization = getOrganization(organizationName);
        if (organization != null) {
            return organization;
        }
        return organizationsApi.createOrganization(requireOrg(organizationName));
    }

    public Organization createOrganization(String organizationName) {
        return organizationsApi.createOrganization(requireOrg(organizationName));
    }

    public Bucket createBucketIfMissing(String bucketName, String organizationName) {
        Bucket bucket = bucketsApi.findBucketByName(requireBucket(bucketName));
        if (bucket != null) {
            return bucket;
        }
        return bucketsApi.createBucket(bucketName, findOrganization(organizationName));
    }


    public Organization getOrganization() {
        return getOrganization(properties.getOrg());
    }

    public Organization getOrganization(String organizationName) {
        String orgName = requireOrg(organizationName);
        return organizationsApi.findOrganizations().stream()
                .filter(item -> orgName.equals(item.getName()))
                .findFirst()
                .orElse(null);
    }

    public List<Organization> listOrganizations() {
        return organizationsApi.findOrganizations();
    }

    public Organization updateOrganization(Organization organization) {
        if (organization == null) {
            throw new IllegalArgumentException("InfluxDB organization must not be null");
        }
        return organizationsApi.updateOrganization(organization);
    }

    public void deleteOrganization() {
        deleteOrganization(properties.getOrg());
    }

    public void deleteOrganization(String organizationName) {
        Organization organization = getOrganization(organizationName);
        if (organization != null) {
            organizationsApi.deleteOrganization(organization);
        }
    }

    public Bucket createBucket(String bucketName, String organizationName) {
        return bucketsApi.createBucket(requireBucket(bucketName), findOrganization(organizationName));
    }

    public Bucket createBucket(String bucketName, String organizationName, int retentionSeconds) {
        BucketRetentionRules retentionRules = new BucketRetentionRules().everySeconds(retentionSeconds);
        return bucketsApi.createBucket(requireBucket(bucketName), retentionRules, requireOrg(organizationName));
    }

    public Bucket getBucket(String bucketName) {
        return bucketsApi.findBucketByName(requireBucket(bucketName));
    }

    public List<Bucket> listBuckets() {
        if (StringUtils.hasText(properties.getOrg())) {
            return bucketsApi.findBucketsByOrgName(properties.getOrg());
        }
        return bucketsApi.findBuckets();
    }

    public Bucket updateBucket(Bucket bucket) {
        if (bucket == null) {
            throw new IllegalArgumentException("InfluxDB bucket must not be null");
        }
        return bucketsApi.updateBucket(bucket);
    }

    public void deleteBucket(String bucketName) {
        Bucket bucket = getBucket(bucketName);
        if (bucket != null) {
            bucketsApi.deleteBucket(bucket);
        }
    }

    public void writePoint(Point point) {
        if (hasExplicitTarget()) {
            writeApiBlocking.writePoint(properties.getBucket(), properties.getOrg(), point);
            return;
        }
        writeApiBlocking.writePoint(point);
    }

    public void writePoints(List<Point> points) {
        if (hasExplicitTarget()) {
            writeApiBlocking.writePoints(properties.getBucket(), properties.getOrg(), points);
            return;
        }
        writeApiBlocking.writePoints(points);
    }

    public <T> void writeMeasurement(WritePrecision precision, T measurement) {
        if (hasExplicitTarget()) {
            writeApiBlocking.writeMeasurement(properties.getBucket(), properties.getOrg(), precision, measurement);
            return;
        }
        writeApiBlocking.writeMeasurement(precision, measurement);
    }

    public void writeRecord(WritePrecision precision, String lineProtocol) {
        if (hasExplicitTarget()) {
            writeApiBlocking.writeRecord(properties.getBucket(), properties.getOrg(), precision, lineProtocol);
            return;
        }
        writeApiBlocking.writeRecord(precision, lineProtocol);
    }

    public <T> void createMeasurement(T measurement) {
        writeMeasurement(WritePrecision.MS, measurement);
    }

    public void createMeasurement(String measurementName,
                                  Map<String, String> tags,
                                  Map<String, Object> fields,
                                  OffsetDateTime time) {
        writePoint(buildPoint(measurementName, tags, fields, time));
    }

    public Point buildPoint(String measurementName,
                            Map<String, String> tags,
                            Map<String, Object> fields,
                            OffsetDateTime time) {
        if (!StringUtils.hasText(measurementName)) {
            throw new IllegalArgumentException("InfluxDB measurement must not be blank");
        }
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("InfluxDB measurement fields must not be empty");
        }
        Point point = Point.measurement(measurementName);
        if (tags != null && !tags.isEmpty()) {
            point.addTags(tags);
        }
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            addField(point, entry.getKey(), entry.getValue());
        }
        if (time != null) {
            point.time(time.toInstant(), WritePrecision.MS);
        }
        return point;
    }

    public List<String> listMeasurements() {
        String flux = "import \"influxdata/influxdb/schema\"\n"
                + "schema.measurements(bucket: \"" + escape(requireBucket(properties.getBucket())) + "\")";
        List<String> measurements = new ArrayList<>();
        for (FluxTable table : query(flux)) {
            for (FluxRecord record : table.getRecords()) {
                Object value = record.getValue();
                if (value instanceof String measurementName) {
                    measurements.add(measurementName);
                }
            }
        }
        return measurements;
    }

    public List<FluxTable> queryMeasurement(String measurementName) {
        return queryMeasurement(measurementName, "-30d", 100);
    }

    public List<FluxTable> queryMeasurement(String measurementName, String rangeStart, int limit) {
        String flux = "from(bucket: \"" + escape(requireBucket(properties.getBucket())) + "\")\n"
                + "  |> range(start: " + requireRangeStart(rangeStart) + ")\n"
                + "  |> filter(fn: (r) => r._measurement == \"" + escape(requireMeasurement(measurementName)) + "\")\n"
                + "  |> limit(n: " + Math.max(1, limit) + ")";
        return query(flux);
    }

    public <T> List<T> queryMeasurement(Class<T> measurementClass, String rangeStart, int limit) {
        String flux = "from(bucket: \"" + escape(requireBucket(properties.getBucket())) + "\")\n"
                + "  |> range(start: " + requireRangeStart(rangeStart) + ")\n"
                + "  |> filter(fn: (r) => r._measurement == \"" + escape(resolveMeasurementName(measurementClass)) + "\")\n"
                + "  |> limit(n: " + Math.max(1, limit) + ")";
        return queryApi.query(flux, requireOrg(properties.getOrg()), measurementClass);
    }

    public List<FluxTable> queryMeasurementPage(String measurementName,
                                                Map<String, String> tags,
                                                Map<String, Object> fields,
                                                String rangeStart,
                                                int pageNo,
                                                int pageSize) {
        int safePageNo = Math.max(1, pageNo);
        int safePageSize = Math.max(1, pageSize);
        int offset = (safePageNo - 1) * safePageSize;
        String flux = "from(bucket: \"" + escape(requireBucket(properties.getBucket())) + "\")\n"
                + "  |> range(start: " + requireRangeStart(rangeStart) + ")\n"
                + "  |> filter(fn: (r) => r._measurement == \"" + escape(requireMeasurement(measurementName)) + "\")\n"
                + buildTagFilters(tags)
                + "  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                + buildFieldFilters(fields)
                + "  |> sort(columns: [\"_time\"], desc: true)\n"
                + "  |> limit(n: " + safePageSize + ", offset: " + offset + ")";
        return query(flux);
    }

    public <T> List<T> queryMeasurementPage(Class<T> measurementClass,
                                            Map<String, String> tags,
                                            Map<String, Object> fields,
                                            String rangeStart,
                                            int pageNo,
                                            int pageSize) {
        int safePageNo = Math.max(1, pageNo);
        int safePageSize = Math.max(1, pageSize);
        int offset = (safePageNo - 1) * safePageSize;
        String flux = "from(bucket: \"" + escape(requireBucket(properties.getBucket())) + "\")\n"
                + "  |> range(start: " + requireRangeStart(rangeStart) + ")\n"
                + "  |> filter(fn: (r) => r._measurement == \"" + escape(resolveMeasurementName(measurementClass)) + "\")\n"
                + buildTagFilters(resolveConditionColumns(measurementClass, tags))
                + "  |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")\n"
                + buildFieldFilters(resolveConditionColumns(measurementClass, fields))
                + "  |> sort(columns: [\"_time\"], desc: true)\n"
                + "  |> limit(n: " + safePageSize + ", offset: " + offset + ")";
        return queryApi.query(flux, requireOrg(properties.getOrg()), measurementClass);
    }

    public List<FluxTable> query(String flux) {
        if (StringUtils.hasText(properties.getOrg())) {
            return queryApi.query(flux, properties.getOrg());
        }
        return queryApi.query(flux);
    }

    public List<FluxTable> query(String flux, Map<String, Object> parameters) {
        return queryApi.query(flux, requireOrg(properties.getOrg()), parameters);
    }

    public <T> List<T> query(String flux, Class<T> mappedType) {
        if (StringUtils.hasText(properties.getOrg())) {
            return queryApi.query(flux, properties.getOrg(), mappedType);
        }
        return queryApi.query(flux, mappedType);
    }

    public String queryRaw(String flux) {
        if (StringUtils.hasText(properties.getOrg())) {
            return queryApi.queryRaw(flux, properties.getOrg());
        }
        return queryApi.queryRaw(flux);
    }

    public void deletePoints(OffsetDateTime start, OffsetDateTime stop, String predicate) {
        deleteApi.delete(start, stop, predicate, requireBucket(properties.getBucket()), requireOrg(properties.getOrg()));
    }

    public void deletePoints(OffsetDateTime start, OffsetDateTime stop, String predicate,
                             String bucketName, String organizationName) {
        deleteApi.delete(start, stop, predicate, requireBucket(bucketName), requireOrg(organizationName));
    }

    public void updatePoint(OffsetDateTime start, OffsetDateTime stop, String predicate, Point point) {
        deletePoints(start, stop, predicate);
        writePoint(point);
    }

    public void deleteMeasurement(String measurementName, OffsetDateTime start, OffsetDateTime stop) {
        deletePoints(start, stop, "_measurement=\"" + escape(requireMeasurement(measurementName)) + "\"");
    }

    public <T> void deleteMeasurement(Class<T> measurementClass, OffsetDateTime start, OffsetDateTime stop) {
        deleteMeasurement(resolveMeasurementName(measurementClass), start, stop);
    }

    public void updateMeasurement(String measurementName,
                                  OffsetDateTime start,
                                  OffsetDateTime stop,
                                  List<Point> points) {
        deleteMeasurement(measurementName, start, stop);
        writePoints(points);
    }

    public void updateMeasurementByCondition(String measurementName,
                                             Map<String, String> tags,
                                             Map<String, Object> fields,
                                             OffsetDateTime start,
                                             OffsetDateTime stop,
                                             List<Point> points) {
        deletePoints(start, stop, buildPredicate(measurementName, tags, fields));
        writePoints(points);
    }

    public <T> void updateMeasurementByCondition(Class<T> measurementClass,
                                                 Map<String, String> tags,
                                                 Map<String, Object> fields,
                                                 OffsetDateTime start,
                                                 OffsetDateTime stop,
                                                 List<T> measurements) {
        deletePoints(
                start,
                stop,
                buildPredicate(
                        resolveMeasurementName(measurementClass),
                        resolveConditionColumns(measurementClass, tags),
                        resolveConditionColumns(measurementClass, fields)));
        if (measurements == null || measurements.isEmpty()) {
            return;
        }
        for (T measurement : measurements) {
            createMeasurement(measurement);
        }
    }

    private Organization findOrganization(String organizationName) {
        String orgName = requireOrg(organizationName);
        return organizationsApi.findOrganizations().stream()
                .filter(item -> orgName.equals(item.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("InfluxDB org does not exist: " + orgName));
    }

    private boolean hasExplicitTarget() {
        return StringUtils.hasText(properties.getBucket()) && StringUtils.hasText(properties.getOrg());
    }

    private String requireBucket(String bucketName) {
        if (!StringUtils.hasText(bucketName)) {
            throw new IllegalStateException("InfluxDB bucket is not configured");
        }
        return bucketName;
    }

    private String requireOrg(String organizationName) {
        if (!StringUtils.hasText(organizationName)) {
            throw new IllegalStateException("InfluxDB org is not configured");
        }
        return organizationName;
    }

    private String requireMeasurement(String measurementName) {
        if (!StringUtils.hasText(measurementName)) {
            throw new IllegalArgumentException("InfluxDB measurement must not be blank");
        }
        return measurementName;
    }

    private String requireRangeStart(String rangeStart) {
        if (!StringUtils.hasText(rangeStart)) {
            throw new IllegalArgumentException("InfluxDB range start must not be blank");
        }
        return rangeStart;
    }

    private void addField(Point point, String key, Object value) {
        if (value instanceof Boolean booleanValue) {
            point.addField(key, booleanValue);
            return;
        }
        if (value instanceof Integer integerValue) {
            point.addField(key, integerValue.longValue());
            return;
        }
        if (value instanceof Long longValue) {
            point.addField(key, longValue);
            return;
        }
        if (value instanceof Float floatValue) {
            point.addField(key, floatValue.doubleValue());
            return;
        }
        if (value instanceof Double doubleValue) {
            point.addField(key, doubleValue);
            return;
        }
        if (value instanceof Number numberValue) {
            point.addField(key, numberValue);
            return;
        }
        point.addField(key, String.valueOf(value));
    }

    private String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String resolveMeasurementName(Class<?> measurementClass) {
        if (measurementClass == null) {
            throw new IllegalArgumentException("InfluxDB measurement class must not be null");
        }
        Measurement measurement = measurementClass.getAnnotation(Measurement.class);
        if (measurement == null || !StringUtils.hasText(measurement.name())) {
            throw new IllegalArgumentException("InfluxDB measurement annotation is missing");
        }
        return measurement.name();
    }

    private <V> Map<String, V> resolveConditionColumns(Class<?> measurementClass, Map<String, V> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return Map.of();
        }
        Map<String, String> columnMappings = resolveColumnMappings(measurementClass);
        Map<String, V> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, V> entry : conditions.entrySet()) {
            resolved.put(columnMappings.getOrDefault(entry.getKey(), entry.getKey()), entry.getValue());
        }
        return resolved;
    }

    private Map<String, String> resolveColumnMappings(Class<?> measurementClass) {
        Map<String, String> mappings = new LinkedHashMap<>();
        for (Field field : measurementClass.getDeclaredFields()) {
            Column column = field.getAnnotation(Column.class);
            if (column != null && StringUtils.hasText(column.name())) {
                mappings.put(field.getName(), column.name());
            }
        }
        return mappings;
    }

    private String buildTagFilters(Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            builder.append("  |> filter(fn: (r) => r[\"")
                    .append(escape(entry.getKey()))
                    .append("\"] == \"")
                    .append(escape(entry.getValue()))
                    .append("\")\n");
        }
        return builder.toString();
    }

    private String buildFieldFilters(Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            builder.append("  |> filter(fn: (r) => r[\"")
                    .append(escape(entry.getKey()))
                    .append("\"] == ")
                    .append(toFluxLiteral(entry.getValue()))
                    .append(")\n");
        }
        return builder.toString();
    }

    private String buildPredicate(String measurementName, Map<String, String> tags, Map<String, Object> fields) {
        StringBuilder builder = new StringBuilder();
        builder.append("_measurement=\"").append(escape(requireMeasurement(measurementName))).append("\"");
        if (tags != null) {
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                builder.append(" AND ")
                        .append(entry.getKey())
                        .append("=\"")
                        .append(escape(entry.getValue()))
                        .append("\"");
            }
        }
        if (fields != null) {
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                builder.append(" AND ")
                        .append(entry.getKey())
                        .append("=")
                        .append(toPredicateLiteral(entry.getValue()));
            }
        }
        return builder.toString();
    }

    private String toFluxLiteral(Object value) {
        if (value instanceof String stringValue) {
            return "\"" + escape(stringValue) + "\"";
        }
        if (value instanceof Boolean || value instanceof Number) {
            return String.valueOf(value);
        }
        return "\"" + escape(String.valueOf(value)) + "\"";
    }

    private String toPredicateLiteral(Object value) {
        if (value instanceof String stringValue) {
            return "\"" + escape(stringValue) + "\"";
        }
        return String.valueOf(value);
    }
}
