/**
 * ==========================================================================
 * MASTER THESIS: HYDROCLIMATIC MONITORING PIPELINE (RAMIS + ILAVE)
 * Data Generation Script (2016-2024)
 * ==========================================================================
 * * Description:
 * This script fetches, pre-processes, and exports multi-source satellite data
 * (Sentinel-1, Sentinel-2, ERA5-Land) for defined Regions of Interest (ROIs).
 * * Output: 
 * CSV files ready for Python ingestion (Pandas).
 */

// --------------------------------------------------------------------------
// 1. CONFIGURATION: GEOGRAPHY & TIME
// --------------------------------------------------------------------------

// Define Study Zones (ROIs)
// Note: IDs are used for filename generation.
var studyZones = [
  // Main Dataset (Target Basin)
  { id: 'Ramis', roi: ee.Geometry.Rectangle([-70.8, -15.4, -69.3, -14.2]), color: 'FF0000' }, // Red

  // External Validation Dataset (Generalization Test)
  { id: 'Ilave', roi: ee.Geometry.Rectangle([-70.8, -16.0, -69.6, -15.5]), color: 'FFFF00' }, // Yellow

  // Spatial Validation Split (Train/Test Split)
  { id: 'Ramis_North', roi: ee.Geometry.Rectangle([-70.8, -14.8, -69.3, -14.2]), color: '0000FF' }, // Blue
  { id: 'Ramis_South', roi: ee.Geometry.Rectangle([-70.8, -15.4, -69.3, -14.8]), color: '008000' }  // Green
];

// Temporal Range
var START_DATE = '2016-01-01';
var END_DATE = '2024-12-31';

// Visualization
Map.setCenter(-70.05, -15.0, 9);
studyZones.forEach(function(z) {
  Map.addLayer(z.roi, { color: z.color }, 'ROI: ' + z.id, false);
});


// --------------------------------------------------------------------------
// 2. PROCESSING FUNCTIONS
// --------------------------------------------------------------------------

/**
 * Sentinel-1 (SAR): Pre-processing and Speckle Filtering
 * Returns: VV and VH backscatter coefficients (dB)
 */
function getS1Collection(roi) {
  return ee.ImageCollection('COPERNICUS/S1_GRD')
    .filterBounds(roi)
    .filterDate(START_DATE, END_DATE)
    .filter(ee.Filter.eq('instrumentMode', 'IW'))
    .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VV'))
    .filter(ee.Filter.listContains('transmitterReceiverPolarisation', 'VH'))
    .filter(ee.Filter.eq('orbitProperties_pass', 'DESCENDING'))
    .map(function(img) {
      // Apply simple median filter to reduce speckle noise
      var clean = img.focal_median(100, 'circle', 'meters').rename(['VV', 'VH']);
      return clean.copyProperties(img, ['system:time_start']);
    });
}

/**
 * Sentinel-2 (Optical): Spectral Indices and Cloud Masking
 * Returns: NDMI, NDVI, NDWI
 */
function getS2Collection(roi) {
  return ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
    .filterBounds(roi)
    .filterDate(START_DATE, END_DATE)
    .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 30))
    .map(function(img) {
      // Scene Classification Map (SCL) for masking
      var scl = img.select('SCL');
      // Keep: Vegetation (4,5), Soil (5), Water (6). Remove clouds/shadows/snow
      var mask = scl.neq(3).and(scl.neq(8)).and(scl.neq(9)).and(scl.neq(10)).and(scl.neq(11));

      // Calculate Normalized Indices
      var ndmi = img.normalizedDifference(['B8', 'B11']).rename('NDMI'); // Moisture
      var ndvi = img.normalizedDifference(['B8', 'B4']).rename('NDVI');  // Vegetation
      var ndwi = img.normalizedDifference(['B3', 'B8']).rename('NDWI');  // Water

      return img.addBands([ndmi, ndvi, ndwi])
        .updateMask(mask)
        .select(['NDMI', 'NDVI', 'NDWI'])
        .copyProperties(img, ['system:time_start']);
    });
}

/**
 * ERA5-Land: Climate Reanalysis Data
 * Returns: Soil Moisture (0-7cm) and Precipitation
 */
function getERA5Collection(roi) {
  var base = ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
    .filterBounds(roi)
    .filterDate(START_DATE, END_DATE)
    .filter(ee.Filter.calendarRange(12, 12, 'hour')); // Sampling at noon

  // Volumetric Soil Water Layer 1
  var sm = base.select('volumetric_soil_water_layer_1')
    .map(function(img) {
      return img.rename('soil_moisture')
        .set('system:time_start', img.get('system:time_start'));
    });

  // Precipitation (Converted from meters to mm)
  var pr = base.select('total_precipitation_hourly')
    .map(function(img) {
      return img.multiply(1000)
        .rename('precipitation_mm')
        .set('system:time_start', img.get('system:time_start'));
    });

  return { sm: sm, pr: pr };
}


// --------------------------------------------------------------------------
// 3. EXPORT MANAGMENT
// --------------------------------------------------------------------------

var SPATIAL_REDUCER = ee.Reducer.mean();
var EXPORT_SCALE = 100; // Resolution in meters

/**
 * Helper function to create Drive Export tasks
 **/
function exportCSV(collection, filename, roi, bandNames) {

  // Reduce images to region stats
  var table = collection.map(function(img) {
    var stats = img.reduceRegion({
      reducer: SPATIAL_REDUCER,
      geometry: roi,
      scale: EXPORT_SCALE,
      bestEffort: true,
      maxPixels: 1e10
    });

    // Set strict date format for easy parsing
    return ee.Feature(null, stats)
      .set('date', img.date().format('YYYY-MM-dd'));
  });

  // Filter out null entries (fully masked images)
  table = table.filter(ee.Filter.notNull(bandNames));

  Export.table.toDrive({
    collection: table,
    description: filename,
    folder: 'GEE_Hydro_Dataset',
    fileFormat: 'CSV',
    selectors: ['date'].concat(bandNames) // Enforce 'date' as first column
  });
}

// --------------------------------------------------------------------------
// 4. EXECUTION LOOP
// --------------------------------------------------------------------------

print('Starting Task Configuration...');

studyZones.forEach(function(zone) {
  print('-> Processing Zone: ' + zone.id);

  // 1. Fetch Collections
  var s1 = getS1Collection(zone.roi);
  var s2 = getS2Collection(zone.roi);
  var era5 = getERA5Collection(zone.roi);

  // 2. Create Export Tasks

  // Sentinel-1 (Radar)
  exportCSV(s1, zone.id + '_S1', zone.roi, ['VV', 'VH']);

  // Sentinel-2 (Indices)
  exportCSV(s2, zone.id + '_S2', zone.roi, ['NDMI', 'NDVI', 'NDWI']);

  // ERA5 (Target: Soil Moisture)
  exportCSV(era5.sm, zone.id + '_ERA5_SM', zone.roi, ['soil_moisture']);

  // ERA5 (Feature: Precipitation)
  exportCSV(era5.pr, zone.id + '_ERA5_PR', zone.roi, ['precipitation_mm']);
});

print('================================================================');
print('SCRIPT FINISHED. PLEASE GO TO THE "TASKS" TAB AND RUN ALL.');
print('================================================================');
