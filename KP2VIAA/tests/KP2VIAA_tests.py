#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from pandas import DataFrame
from KP2VIAA.KP2VIAA import KP2VIAA
from json import load
from codecs import open


class KP2VIAATests(TestCase):
    def setUp(self):
        self.kp2viaa = KP2VIAA(path_to_dbcfg="../resources/db.cfg",
                               path_to_viaa2kp="../resources/viaa_id_testcase.json")
        self.maxDiff = None

    def test_read_mapping(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        expected_mapping = {
            "viaa_id": {
                "kp_productie_id": 429013,
                "kp_show_id": "kp_show_id"
            }
        }
        self.assertDictEqual(self.kp2viaa.viaa_id_to_kp_productie_show_id_mapping, expected_mapping)

    def test_dbcfg_read_in_correctly(self):
        self.assertEqual(self.kp2viaa.path_to_dbcfg, "../resources/db.cfg")
        self.assertTrue("db" in self.kp2viaa.cfg)

    def test_get_metadata_for_mozart(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_for_viaa_id("viaa_id")
        mozart_metadata_expected = DataFrame([
            {
                "name": "Mozart/Concert Arias Un Moto di Gioia",
                "season": "1992-1993",
                "rerun": None
            }
        ], columns=["name", "season", "rerun"])
        self.assertTrue(self.kp2viaa.general_info.equals(mozart_metadata_expected))

    def test_get_metadata_for_mozart_functies(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        with open("../resources/mozart_personen_functies.json", "r", "utf-8") as f:
            x = load(f, encoding='utf-8')
        mozart_metadata_expected = DataFrame(data=x, columns=['function', "production id", 'full name'])
        self.assertEqual(self.kp2viaa.people_info.to_string(), mozart_metadata_expected.to_string())

    def test_get_metadata_for_mozart_organisaties(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_organisaties_for_viaa_id("viaa_id")
        mozart_metadata_expected = DataFrame([
            {
                "organisatie": "Rosas",
                "functie": "gezelschap"
            }
        ], columns=["organisatie", "functie"])
        self.assertTrue(self.kp2viaa.organisations_info.equals(mozart_metadata_expected))
