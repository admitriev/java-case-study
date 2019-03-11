package com.trivago.mp.casestudy;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

public class CsvDataLoader {
    public static Stream<CSVRecord> load(Path dataFilePath) {
        try (CSVParser parser = new CSVParser(
                new FileReader(dataFilePath.toFile()),
                CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            return parser.getRecords().stream();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Map<Integer, String> readCities(Stream<CSVRecord> stream) {
        return stream.collect(Collectors.toMap(rec -> parseInt(rec.get("id")), rec -> rec.get("city_name")));
    }

    public static Map<Integer, Set<Integer>> readAdvertiserHotel(Stream<CSVRecord> stream) {
        Map<Integer, Set<Integer>> result = new HashMap<>();
        Iterator<CSVRecord> iterator = stream.iterator();
        while (iterator.hasNext()) {
            CSVRecord next = iterator.next();
            int advertiserId = parseInt(next.get("advertiser_id"));
            int hotelId = parseInt(next.get("hotel_id"));
            result.computeIfAbsent(advertiserId, k -> new HashSet<>()).add(hotelId);
        }
        return result;
    }

    public static Map<Integer, Advertiser> readAdvertisers(Stream<CSVRecord> stream) {
        return stream.collect(Collectors.toMap(rec -> parseInt(rec.get("id")), rec ->
                new Advertiser(parseInt(rec.get("id")), rec.get("advertiser_name"))));
    }

    public static Map<Integer, Hotel> readHotels(
            Stream<CSVRecord> stream,
            Map<Integer, Long> clicks,
            Map<Integer, Long> impressions,
            Map<String, Set<Integer>> hotelsByCity,
            Map<Integer, String> cityById) {
        Map<Integer, Hotel> result = new HashMap<>();
        Iterator<CSVRecord> iterator = stream.iterator();
        while (iterator.hasNext()) {
            CSVRecord next = iterator.next();

            Hotel hotel = toHotel(next);
            result.put(hotel.getId(), hotel);

            clicks.put(hotel.getId(), parseLong(next.get("clicks")));
            impressions.put(hotel.getId(), parseLong(next.get("impressions")));

            int cityId = parseInt(next.get("city_id"));
            String cityName = cityById.get(cityId);
            Objects.requireNonNull(cityName, "No city found for id = " + cityId);
            hotelsByCity.computeIfAbsent(cityName, k -> new HashSet<>()).add(hotel.getId());
        }
        return result;
    }

    private static Hotel toHotel(CSVRecord record) {
        return new Hotel(
                parseInt(record.get("id")),
                record.get("name"),
                parseInt(record.get("rating")),
                parseInt(record.get("stars")));
    }
}
