package com.trivago.mp.casestudy;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static com.trivago.mp.casestudy.CsvDataLoader.load;

/**
 * Your task will be to implement two functions, one for loading the data which is stored as .csv files in the ./data
 * folder and one for performing the actual search.
 */
public class HotelSearchEngineImpl implements HotelSearchEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotelSearchEngineImpl.class);

    private Map<Integer, String> cities;

    private Map<Integer, Advertiser> advertisers;
    private Map<Integer, Set<Integer>> hotelsByAdvertiser;

    private Map<Integer, Hotel> hotels;
    private Map<String, Set<Integer>> hotelsByCity;
    private Map<Integer, Long> hotelsClicks;
    private Map<Integer, Long> hotelsImpressions;

    @Override
    public void initialize() {
        Path dataBaseDir = Paths.get("data");

        cities = CsvDataLoader.readCities(load(dataBaseDir.resolve("cities.csv")));

        advertisers = CsvDataLoader.readAdvertisers(load(dataBaseDir.resolve("advertisers.csv")));
        hotelsByAdvertiser = CsvDataLoader.readAdvertiserHotel(load(dataBaseDir.resolve("hotel_advertiser.csv")));

        Map<Integer, Long> clicks = new HashMap<>();
        Map<Integer, Long> impressions = new HashMap<>();
        Map<String, Set<Integer>> hotelsByCity = new HashMap<>();
        hotels = CsvDataLoader.readHotels(load(dataBaseDir.resolve("hotels.csv")), clicks, impressions, hotelsByCity, cities);
        hotelsClicks = clicks;
        hotelsImpressions = impressions;
        this.hotelsByCity = hotelsByCity;
        LOGGER.info("Initialized with {} cities, {} advertisers, {} hotels", cities.size(), advertisers.size(), hotels.size());
        // we can build some other auxiliary structures to increase searching
        // but for the sake of simplicity it will be skipped right now
    }

    @Override
    public List<HotelWithOffers> performSearch(String cityName, DateRange dateRange, OfferProvider offerProvider) {
        Set<Integer> hotelsIds = hotelsByCity.getOrDefault(cityName, Collections.emptySet());
        if (hotelsIds.isEmpty() || dateRange.getStartDate() >= dateRange.getEndDate()) {
            LOGGER.debug("Start date >= end date {} - skip searching", dateRange);
            return Collections.emptyList();
        }

        LOGGER.debug("Preforming search for {} hotels in {} city", hotelsIds, cityName);
        Map<Integer, List<Offer>> offersByHotelId = requestOffers(dateRange, offerProvider, hotelsIds);
        // actually we should use CPC and Price to leave only one offer for the given hotel
        // but there is no requirement for the usage of CPC and Price in #performSearch
        // so it is implemented as stated in javadoc
        List<HotelWithOffers> result = offersByHotelId.entrySet().stream().map(entry -> {
            HotelWithOffers hotelWithOffers = new HotelWithOffers(hotels.get(entry.getKey()));
            hotelWithOffers.setOffers(entry.getValue());
            return hotelWithOffers;
        }).collect(Collectors.toList());
        LOGGER.debug("Found {} offers for hotels in {} city", result.size(), cityName);
        return result;
    }

    private Map<Integer, List<Offer>> requestOffers(DateRange dateRange, OfferProvider offerProvider, Set<Integer> hotelsIds) {
        Map<Integer, List<Offer>> offersByHotelId = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> hotelsByAdvertiser: hotelsByAdvertiser.entrySet()) {
            // use full scan due to small size of the map
            int advertiserId = hotelsByAdvertiser.getKey();
            Set<Integer> advertiserHotelsIds = hotelsByAdvertiser.getValue();
            List<Integer> advertisersHotelsInCity = advertiserHotelsIds
                    .stream()
                    .filter(hotelsIds::contains)
                    .collect(Collectors.toList());
            if (advertiserHotelsIds.isEmpty()) {
                continue;
            }
            // it could be done in parallel threads to decrease response time
            // first we can request some offers for best rated hotels but it is not worth diving into premature optimisations
            // caching data is one of the optimisation too - leave it for the future dev
            LOGGER.debug("Requesting offers from advertiser {}", advertiserId);
            Map<Integer, Offer> offersFromAdvertiser = offerProvider.getOffersFromAdvertiser(
                    advertisers.get(advertiserId),
                    advertisersHotelsInCity,
                    dateRange);
            LOGGER.debug("Advertise {} has offered {} hotels", advertiserId, offersFromAdvertiser.size());
            offersFromAdvertiser.forEach((k, v) -> offersByHotelId.computeIfAbsent(k, key -> new ArrayList<>()).add(v));
        }
        return offersByHotelId;
    }
}
