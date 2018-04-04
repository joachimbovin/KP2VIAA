#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unittest import TestCase
from pandas import DataFrame
from KP2VIAA.KP2VIAA import KP2VIAA
from json import load
from codecs import open
from lxml import etree


class KP2VIAATests(TestCase):
    def setUp(self):
        self.kp2viaa = KP2VIAA(path_to_dbcfg="../resources/db.cfg",
                               path_to_viaa2kp="../resources/viaa_id_testcase.json",
                               path_to_xml="../resources/test_file.xml",
                               path_metadata_mapping="../resources/metadata_mapping.json",
                               path_genres_mapping="../resources/genres_mapping.json",
                               path_languages_mapping="../resources/languages_mapping.json",
                               path_to_qas_auth="../resources/qasviaaauthenticationbase64.txt")
        self.maxDiff = None

    def test_map_kp_general_to_dc_titles(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_general_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists("dc_titles")
        self.kp2viaa.map_kp_general_to_viaa()
        self.assertTrue(len(self.kp2viaa.tree.xpath("//seizoen")), 1)
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))


    def test_ensure_element_exists(self):
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists("testje")
        self.assertEqual(len(self.kp2viaa.tree.xpath("//testje")), 1)

    def test_map_kp_persons_to_viaa_makers(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists("dc_creators")
        self.kp2viaa.map_kp_persons_to_viaa_makers("viaa_id")
        self.assertEqual(self.kp2viaa.tree.xpath("//Choreograaf")[0].text, "Anne Teresa De Keersmaeker")
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))


    def test_map_kp_persons_to_viaa_contributors(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists('dc_contributors')
        self.kp2viaa.map_kp_persons_to_viaa_contributors("viaa_id")
        self.assertEqual(self.kp2viaa.tree.xpath("//Dirigent")[0].text, "Philippe Herreweghe")
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))

    def test_map_kp_organisations_to_viaa_makers(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_organisaties_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists('dc_creators')
        self.kp2viaa.map_kp_organisations_to_viaa_makers("viaa_id")
        self.assertEqual(self.kp2viaa.tree.xpath("//Maker")[0].text, "Rosas")
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))


    #no test for organisations to viaa contributors because for this there is no relevant metadata

    def test_map_kp_organisations_to_viaa_makers(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_organisaties_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists('dc_contributors')
        self.kp2viaa.map_kp_organisations_to_viaa_contributors("viaa_id")
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))


    def test_map_kp_genres_to_viaa_genres(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists('dc_types')
        self.kp2viaa.write_kp_genres_to_viaa_genres("viaa_id")
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))


    def test_map_kp_languages_to_viaa_languages(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_languages_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists("dc_languages")
        self.kp2viaa.write_kp_languages_to_viaa_languages("viaa_id")
        print(etree.tostring(self.kp2viaa.tree,pretty_print=True))

    def test_write_all(self):
        self.kp2viaa.get_kp_metadata_general_for_viaa_id("viaa_id")
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.ensure_element_exists("dc_titles")
        self.kp2viaa.ensure_element_exists('dc_creators')
        self.kp2viaa.ensure_element_exists('dc_contributors')
        self.kp2viaa.ensure_element_exists('dc_types')
        self.kp2viaa.ensure_element_exists("dc_languages")
        self.kp2viaa.map_kp_general_to_viaa()
        self.kp2viaa.map_kp_persons_to_viaa_makers("viaa_id")
        self.kp2viaa.map_kp_persons_to_viaa_contributors("viaa_id")
        self.kp2viaa.map_kp_organisations_to_viaa_makers("viaa_id")
        self.kp2viaa.map_kp_organisations_to_viaa_contributors("viaa_id")
        self.kp2viaa.write_kp_genres_to_viaa_genres("viaa_id")
        self.kp2viaa.write_kp_languages_to_viaa_languages("viaa_id")
        print(etree.tostring(self.kp2viaa.tree,pretty_print=True))

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
        self.kp2viaa.get_kp_metadata_general_for_viaa_id("viaa_id")
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

    def test_get_metadata_for_mozart_genres(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
        mozart_metadata_expected = DataFrame([
            {
                "Voorstelling" : "Mozart/Concert Arias Un Moto di Gioia",
                "Genre" : "dans"
            }
        ], columns=["Voorstelling", "Genre"])
        self.assertTrue(self.kp2viaa.genre_info.equals(mozart_metadata_expected))

    def test_get_metadata_for_mozart_languages(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_languages_for_viaa_id("viaa_id")
        mozart_metadata_expected = DataFrame([
            {
                "voorstelling" : "Mozart/Concert Arias Un Moto di Gioia",
                "taal" : None
            }
        ], columns=["voorstelling", "taal"])
        self.assertTrue(self.kp2viaa.language_info.equals(mozart_metadata_expected))

    def test_consume_api(self):
        #self.kp2viaa.consume_api("d9e8142d64714b2ab9081317f7ef0c64a33b914162b34b25a5ab91ba192181c744fb015640ec43c9be820ab05ad4a42e")
        self.kp2viaa.consume_api("bv79s1r49d")

    def test_test_PID(self):   #!!!
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.test_if_PID_unique()

    def test_append_title(self):
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.get_title_mediahaven()
        print(etree.tostring(self.kp2viaa.tree, pretty_print=True))

    def test_find_mediahaven(self):
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.read_viaa_xml_to_tree()
        self.kp2viaa.is_in_mediahaven("Anne Teresa De Keersmaeker" , "choreografie")



