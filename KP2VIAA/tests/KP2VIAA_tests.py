from unittest import TestCase
from KP2VIAA.KP2VIAA import KP2VIAA
from pandas import DataFrame


class KP2VIAATests(TestCase):
    def setUp(self):
        self.kp2viaa = KP2VIAA()

    def test_read_mapping(self):
        self.kp2viaa.read_mapping_VIAA_to_KP()
        expected_mapping = {
            "viaa_id": {
                "kp_productie_id": "kp_prod_id",
                "kp_show_id": "kp_show_id"
            }
        }
        self.assertDictEqual(self.kp2viaa.mapping, expected_mapping)

    def test_get_metadata_for_vortex_temporum(self):
        vortex_temporum_metadata = self.kp2viaa.get_KP_metadata_for_VIAA_id("viaa_id")
        self.assertTrue(isinstance(vortex_temporum_metadata, DataFrame))