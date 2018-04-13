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
                               path_metadata_mapping="../resources/metadata_mapping.json",
                               path_genres_mapping="../resources/genres_mapping.json",
                               path_languages_mapping="../resources/languages_mapping.json",
                               path_to_qas_auth="../resources/qasviaaauthenticationbase64.txt",
                               path_to_xsd="../resources/viaa_metadatamodel_van_viaa_naar_mam.xsd")
        self.maxDiff = None

    def test_map_kp_general_to_dc_titles(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_general_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists("dc_titles")
        self.kp2viaa.write_kp_general_to_update_tree()
        self.assertTrue(len(self.kp2viaa.update_tree.xpath("//seizoen")), 1)
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))


    def test_ensure_element_exists(self):
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists("testje")
        self.assertEqual(len(self.kp2viaa.update_tree.xpath("//testje")), 1)

    def test_map_kp_persons_to_viaa_makers(self):
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists("dc_creators")
        self.kp2viaa.write_kp_persons_to_viaa_makers("viaa_id")
        self.assertEqual(self.kp2viaa.update_tree.xpath("//Choreograaf")[0].text, "Anne Teresa De Keersmaeker")
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))


    def test_map_kp_persons_to_viaa_contributors(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists('dc_contributors')
        self.kp2viaa.write_kp_persons_to_viaa_contributors("viaa_id")
        self.assertEqual(self.kp2viaa.update_tree.xpath("//Dirigent")[0].text, "Philippe Herreweghe")
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))

    def test_map_kp_organisations_to_viaa_makers(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_organisaties_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists('dc_creators')
        self.kp2viaa.write_kp_organisations_to_viaa_makers("viaa_id")
        self.assertEqual(self.kp2viaa.update_tree.xpath("//Maker")[0].text, "Rosas")
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))


    #no test for organisations to viaa contributors because there is no relevant metadata for this

    def test_map_kp_organisations_to_viaa_contributors(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_organisaties_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists('dc_contributors')
        self.kp2viaa.write_kp_organisations_to_viaa_contributors("viaa_id")
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))


    def test_map_kp_genres_to_viaa_genres(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists('dc_types')
        self.kp2viaa.write_kp_genres_to_viaa_genres("viaa_id")
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))


    def test_map_kp_languages_to_viaa_languages(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_languages_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists("dc_languages")
        self.kp2viaa.write_kp_languages_to_viaa_languages("viaa_id")
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))

    def test_write_all(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.get_kp_metadata_general_for_viaa_id("viaa_id")
        self.kp2viaa.get_kp_metadata_organisaties_for_viaa_id("viaa_id")
        self.kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
        self.kp2viaa.get_kp_metadata_languages_for_viaa_id("viaa_id")
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.create_viaa_xml()     # = create tree!
        self.kp2viaa.ensure_element_exists("dc_titles")
        self.kp2viaa.ensure_element_exists('dc_creators')
        self.kp2viaa.ensure_element_exists('dc_contributors')
        self.kp2viaa.ensure_element_exists('dc_types')
        self.kp2viaa.ensure_element_exists("dc_languages")
        #self.kp2viaa.get_title_mediahaven()    # laten vallen!
        self.kp2viaa.write_kp_general_to_update_tree()
        self.kp2viaa.write_kp_persons_to_viaa_makers()
        self.kp2viaa.write_kp_persons_to_viaa_contributors()
        self.kp2viaa.write_kp_organisations_to_viaa_makers()
        self.kp2viaa.write_kp_organisations_to_viaa_contributors()
        self.kp2viaa.write_kp_languages_to_viaa_languages()
        self.kp2viaa.write_kp_genres_to_viaa_genres()
        #print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True, encoding="utf-8"))
        self.kp2viaa.send_update_tree_to_viaa("viaa_id")

    def test_encoding(self):
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        encoded_name = self.kp2viaa.people_info.ix[18]["full name"]

        self.kp2viaa.create_viaa_xml()
        child = etree.Element("test")
        child.text = encoded_name.decode("utf-8")
        self.kp2viaa.update_tree.append(child)

        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True, encoding="utf-8"))

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
        #self.kp2viaa.consume_api("349a8b45a8bc47fdb214f79794c24994234453a83ff0453cbad2197e23a292a3d423bd21a938495ea0a19c92e2c02bc5")
        self.kp2viaa.consume_api("s17sn05s9c")
        print(etree.tostring(self.kp2viaa.mediahaven_xml, pretty_print=True))

    def test_test_PID(self):
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.test_if_PID_unique()

    def test_append_title(self):
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.get_title_mediahaven()
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))

    def test_find_mediahaven(self):
        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.is_in_mediahaven("Anne Teresa De Keersmaeker" , "choreografie")

    def test_compare_kp_mediahaven_personen_1(self):
        """
        Existing person and existing function
        :return: True (don't write) >>> edited to check > should return false but doesn't !!!!!!!!!
        """
        self.kp2viaa.consume_api("bv79s1r49d")
        element = list(self.kp2viaa.mediahaven_xml.iter('mdProperties'))[0]
        #dc_creators = etree.parse("../resources/mediahaven_test.xml")
        new_person = etree.fromstring('''
        <root>
        <mdProperty>
        <key>dc_creators</key>
        <value>
        <value>
        <key>Danser</key>
        <value>Anne Teresa De Keersmaeker</value>
        </value>
        <value>
        <key>Choreograaf</key>
        <value>Tom Ruette</value>
        </value>
        </value>
        </mdProperty>
        </root>''')
        for item in new_person:
            element.append(item)
        #print(etree.tostring(self.kp2viaa.mediahaven_xml, pretty_print=True))
        #self.kp2viaa.is_in_mediahaven("Anne Teresa De Keersmaeker", "Choreograaf")
        self.assertTrue(self.kp2viaa.compare_mediahaven_kunstenpunt("Anne Teresa De Keersmaeker", "Choreograaf"))
        #SHOULD RETURN FALSE!

    def test_compare_kp_mediahaven_personen_2(self):
        """
        Tests for new person + existing function
        :return: False (write)
        """
        self.kp2viaa.consume_api("bv79s1r49d")
        element = list(self.kp2viaa.mediahaven_xml.iter('mdProperties'))[0]
        new_person = etree.fromstring('''
        <root>
        <mdProperty>
        <key>dc_creators</key>
        <value>
        <value>
        <key>Choreograaf</key>
        <value>Tom Ruette</value>
        </value>
        </value>
        </mdProperty>
        </root>''')
        for item in new_person:
            element.append(item)
        self.assertFalse(self.kp2viaa.compare_mediahaven_kunstenpunt("Anne Teresa De Keersmaeker", "Choreograaf"))


    def test_compare_kp_mediahaven_personen_3(self):
        """
        Tests for existing person + new function
        :return: False (write)
        """
        self.kp2viaa.consume_api("bv79s1r49d")
        element = list(self.kp2viaa.mediahaven_xml.iter('mdProperties'))[0]
        new_person = etree.fromstring('''
        <root>
        <mdProperty>
        <key>dc_creators</key>
        <value>
        <value>
        <key>Fotograaf</key>
        <value>Anne Teresa De Keersmaeker</value>
        </value>
        </value>
        </mdProperty>
        </root>''')
        for item in new_person:
            element.append(item)
        self.assertFalse(self.kp2viaa.compare_mediahaven_kunstenpunt("Anne Teresa De Keersmaeker", "Choreograaf"))


    def test_compare_kp_mediahaven_personen_4(self):
        """
        Tests for new person + new function
        :return: False (write)
        """
        self.kp2viaa.consume_api("bv79s1r49d")
        element = list(self.kp2viaa.mediahaven_xml.iter('mdProperties'))[0]
        new_person = etree.fromstring('''
        <root>
        <mdProperty>
        <key>dc_creators</key>
        <value>
        <value>
        <key>Fotograaf</key>
        <value>Tom Ruette</value>
        </value>
        </value>
        </mdProperty>
        </root>''')
        for item in new_person:
            element.append(item)
        self.assertFalse(self.kp2viaa.compare_mediahaven_kunstenpunt("Anne Teresa De Keersmaeker", "Choreograaf"))



    def test_compare_kp_mediahaven_personen_write_to_xml(self):
        """
        Anne Teresa De Keersmaeker is already in VIAA xml, test returns True. We don't write. <dc_creators>
        should not contain Anne Teresa De Keersmaeker as Choreograaf
        :return: True (don't write)
        """
        self.kp2viaa.consume_api("bv79s1r49d")
        element = list(self.kp2viaa.mediahaven_xml.iter('mdProperties'))[0]
        new_person = etree.fromstring('''
        <root>
        <mdProperty>
        <key>dc_creators</key>
        <value>
        <value>
        <key>Choreograaf</key>
        <value>Anne Teresa De Keersmaeker</value>
        </value>
        </value>
        </mdProperty>
        </root>''')
        for item in new_person:
            element.append(item)
        #print(etree.tostring(self.kp2viaa.mediahaven_xml, pretty_print=True))
        print self.kp2viaa.compare_mediahaven_kunstenpunt("Anne Teresa De Keersmaeker", "Choreograaf")
        self.kp2viaa.read_mapping_viaa_to_kp()
        self.kp2viaa.get_kp_metadata_personen_for_viaa_id("viaa_id")
        self.kp2viaa.create_viaa_xml()
        self.kp2viaa.ensure_element_exists("dc_creators")
        self.kp2viaa.write_kp_persons_to_viaa_makers("viaa_id")
        print(etree.tostring(self.kp2viaa.mediahaven_xml, pretty_print=True))
        print(etree.tostring(self.kp2viaa.update_tree, pretty_print=True))




    def test_validate_kp_input_to_viaa_xsd(self):

        self.test_write_all()
        #self.kp2viaa.validate_updated_tree_to_VIAA_xsd()
        self.assertTrue(self.kp2viaa.validate_updated_tree_to_VIAA_xsd())

    def test_get_mediahaven_fragmentID(self):

        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.get_mediahaven_fragmentId()

    def test_send_to_API(self):

        self.kp2viaa.consume_api("bv79s1r49d")
        self.kp2viaa.send_update_tree_to_viaa()






