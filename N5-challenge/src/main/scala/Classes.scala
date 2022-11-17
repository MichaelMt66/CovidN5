
// RAW SCHEMA
case class CountryRaw(provinceState: String,
                      countryRegion: String,
                      lat: String,
                      lon: String,
                      region: String)

case class CityRaw(UID: String,
                   iso2: String,
                   iso3: String,
                   code3: String,
                   FIPS: String,
                   admin2: String,
                   provinceState: String,
                   countryRegion: String,
                   lat: String,
                   lon: String,
                   combinedKey: String)

case class CovidTrackRaw(UID: String,
                         dataDate: String,
                         confirmed: String,
                         deaths: String)


case class Country(provinceState: String,
                   countryRegion: String,
                   lat: Double,
                   lon: Double,
                   region: String)

case class City(UID: String,
                iso2: String,
                iso3: String,
                code3: String,
                FIPS: Float,
                admin2: String,
                provinceState: String,
                countryRegion: String,
                lat: Double,
                lon: Double,
                combinedKey: String)

case class CovidTrack(UID: String,
                      dataDate: String,
                      confirmed: Long,
                      deaths: Long)