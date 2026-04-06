import pytest

from gfv2_params.download.copernicus_dem import tile_label, tiles_for_bbox


class TestTileLabel:
    def test_north_west(self):
        assert tile_label(48, -123) == "Copernicus_DSM_COG_10_N48_00_W123_00_DEM"

    def test_north_east(self):
        assert tile_label(48, 10) == "Copernicus_DSM_COG_10_N48_00_E010_00_DEM"

    def test_south_west(self):
        assert tile_label(-5, -70) == "Copernicus_DSM_COG_10_S05_00_W070_00_DEM"

    def test_zero_zero(self):
        assert tile_label(0, 0) == "Copernicus_DSM_COG_10_N00_00_E000_00_DEM"

    def test_negative_lat_negative_lon(self):
        assert tile_label(-33, -71) == "Copernicus_DSM_COG_10_S33_00_W071_00_DEM"

    def test_padding(self):
        # Single-digit lat gets zero-padded to 2 digits, lon to 3
        label = tile_label(5, -8)
        assert "N05" in label
        assert "W008" in label


class TestTilesForBbox:
    def test_single_tile(self):
        labels = tiles_for_bbox(48.0, 48.5, -123.0, -122.5)
        assert len(labels) == 1
        assert labels[0] == tile_label(48, -123)

    def test_four_tiles(self):
        # 2x2 grid: 48-50N, 123-121W
        labels = tiles_for_bbox(48.0, 49.5, -123.0, -121.5)
        assert len(labels) == 4

    def test_exact_degree_boundary(self):
        # Bbox from exactly 48.0 to 49.0 should cover tiles at lat 48 and 49
        labels = tiles_for_bbox(48.0, 49.0, -123.0, -122.0)
        lats = {48, 49}
        lons = {-123, -122}
        assert len(labels) == len(lats) * len(lons)

    def test_no_duplicates(self):
        labels = tiles_for_bbox(41.0, 55.0, -141.0, -52.0)
        assert len(labels) == len(set(labels))

    def test_canada_zone_count(self):
        # Canada border zone: 41-55N, 141-52W
        labels = tiles_for_bbox(41.0, 55.0, -141.0, -52.0)
        expected_lats = 55 - 41 + 1  # 15
        expected_lons = -52 - (-141) + 1  # 90
        assert len(labels) == expected_lats * expected_lons

    def test_mexico_zone_count(self):
        # Mexico border zone: 25-33N, 118-96W
        labels = tiles_for_bbox(25.0, 33.0, -118.0, -96.0)
        expected_lats = 33 - 25 + 1  # 9
        expected_lons = -96 - (-118) + 1  # 23
        assert len(labels) == expected_lats * expected_lons
