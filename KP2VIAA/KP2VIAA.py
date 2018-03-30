# -*- coding: utf-8 -*-

from json import load, dumps
from codecs import open
import psycopg2
from configparser import ConfigParser
from pandas import DataFrame
from lxml import etree
import requests


class KP2VIAA(object):
    def __init__(self, path_to_dbcfg="resources/db.cfg",
                 path_to_viaa2kp="resources/viaa_id_testcase.json",
                 path_to_xml="resources/test_file.xml",
                 path_metadata_mapping="resources/metadata_mapping.json",
                 path_to_qas_auth="resources/qasviaaauthenticationbase64.txt"):
        self.path_to_viaa2kp_mapping = path_to_viaa2kp
        self.path_to_xml = path_to_xml
        self.path_metadata_mapping = path_metadata_mapping
        self.path_to_qas_auth=path_to_qas_auth
        self.viaa_id_to_kp_productie_show_id_mapping = None
        self.path_to_dbcfg = path_to_dbcfg
        self.cfg = ConfigParser()
        self.cfg.read(self.path_to_dbcfg)
        self.knst = psycopg2.connect(host=self.cfg['db']['host'],
                                     port=self.cfg['db']['port'],
                                     database=self.cfg['db']['db'],
                                     user=self.cfg['db']['user'],
                                     password=self.cfg['db']['pwd'])
        self.knst.set_client_encoding('UTF-8')

        self.general_info = None
        self.people_info = None
        self.organisations_info = None
        self.genre_info = None
        self.language_info = None
        self.tree = None

    def read_mapping_viaa_to_kp(self):
        """
        This function reads in the handmade mapping from VIAA object ids to Kunstenpunt production ids and show ids
        {
          viaa_id:
            {
              kp_productie: kp_productie_id,
              kp_show_id: kp_show_id
            }
        }
        """
        with open(self.path_to_viaa2kp_mapping, "r", "utf-8") as f:
            self.viaa_id_to_kp_productie_show_id_mapping = load(f)

    def get_access_database(self):
        """
        Gets access to the KP database
        :return: a cursor object to interact with the MySQL server using a MySQLConnection object.
        """
        cur = self.knst.cursor()
        return cur   #do I still have to return cur?

    def get_kp_metadata_general_for_viaa_id(self, viaa_id):
        """
        Returns the kunstenpunt metadata about the production for a specific viaa_id. The viaa id is mapped to a kunstenpunt production id
        and a kunstenpunt show id.
        :param viaa_id: the original viaa id
        :return: A pandas object containing all the kunstenpunt metadata for the production and potentially the show.
        """
        cur = self.get_access_database()
        productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT pr.title, seasons.name, pr.rerun_of_id
        FROM production.productions AS pr
        JOIN production.seasons AS seasons
        ON pr.season_id = seasons.id
        WHERE pr.id={0}
        """.format(productie_id)
        cur.execute(sql)
        rosas_productions = cur.fetchall()
        self.general_info = DataFrame(rosas_productions, columns=['name', 'season', 'rerun'])

    def get_kp_metadata_personen_for_viaa_id(self, viaa_id):
        """
        Returns the KP metadata about the persons and their function based on the VIAA ID.
        :param viaa_id:
        :return: A pandas object containing all the kunstenpunt metadata for the persons and their functions.
        """
        cur = self.get_access_database()
        productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT people.full_name, fun.name_nl, pr.id
        FROM production.productions AS pr
        JOIN production.relationships AS rel
        ON pr.id = rel.production_id
        JOIN production.people as people
        ON rel.person_id = people.id
        JOIN production.functions AS fun
        ON rel.function_id = fun.id
        WHERE pr.id={0}
          """.format(productie_id)
        cur.execute(sql)
        rosas_productions = cur.fetchall()
        self.people_info = DataFrame(rosas_productions, columns=['full name', 'function', 'production id'])

    def get_kp_metadata_organisaties_for_viaa_id(self, viaa_id):
        """
        Returns the KP metadata about the organisations and their function based on the VIAA ID.
        :param viaa_id:
        :return:
        """
        cur = self.get_access_database()
        productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT organisations.name, functions.name_nl
        FROM production.productions AS pr
        JOIN production.relationships AS rel_prod_org
        ON pr.id = rel_prod_org.production_id
        JOIN production.organisations as organisations
        ON rel_prod_org.organisation_id = organisations.id
        JOIN production.functions as functions
        ON rel_prod_org.function_id = functions.id
        WHERE pr.id={0}
        """.format(productie_id)
        cur.execute(sql)
        rosas_productions = cur.fetchall()
        self.organisations_info = DataFrame(rosas_productions, columns=['organisatie', 'functie'])

    def get_kp_metadata_genres_for_viaa_id(self, viaa_id):
        """
        Return the KP metadata about the genre based on the VIAA ID
        :param viaa_id:
        :return:
        """
        cur = self.get_access_database()
        productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT pr.title, genres.name_nl
        FROM production.productions AS pr
        JOIN production.relationships AS prod_rel_genre
        ON pr.id = prod_rel_genre.production_id
        JOIN production.genres AS genres
        ON prod_rel_genre.genre_id = genres.id
        WHERE pr.id={0}    
        """.format(productie_id)
        cur.execute(sql)
        rosas_productions = cur.fetchall()
        self.genre_info = DataFrame(rosas_productions, columns=['Voorstelling','Genre'])

    def get_kp_metadata_languages_for_viaa_id(self, viaa_id):
        """
        Creates class variable containing DataFrame with kp metadata about languages based on the viaa id
        :param viaa_id:
        :return: metadata languages
        """
        cur = self.get_access_database()
        productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """SELECT pr.title, lang.name_nl
        FROM production.productions AS pr
        LEFT JOIN production.production_languages AS prod_lang
        ON prod_lang.production_id = pr.id
        LEFT JOIN production.languages as lang
        ON prod_lang.language_id = lang.id
        WHERE pr.id={0}
        """.format(productie_id)
        cur.execute(sql)
        rosas_productions = cur.fetchall()
        self.language_info = DataFrame(rosas_productions, columns=['voorstelling','taal'])

    def consume_api(self, viaa_id):
        with open(self.path_to_qas_authentication, "r") as f:
            base64pass = f.read()
        header = {
            "Accept": "application/xml",
            "Authorization": "Basic " + base64pass
        }
        url = "https://archief-qas.viaa.be/mediahaven-rest-api/resources/media/{0}".format(viaa_id)
        r = requests.get(url, headers=header)
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        tree = etree.fromstring(r.text.encode("utf-8"), parser=parser)
        print(etree.tostring(tree, pretty_print=True))



    def read_viaa_xml_to_tree(self):
        """
        Reads the viaa xml file to a xml tree
        :return: class variable xml tree
        """
        parser = etree.XMLParser(remove_blank_text=True)
        with open(self.path_to_xml) as file:   #change this to parameter xml_viaa
            self.tree = etree.parse(file, parser)

    def write_kp_general_to_dc_titles(self, name_tag_viaa, tag_kp):
        """

        :param name_tag_viaa:
        :param tag_kp:
        :return:
        """
        element = list(self.tree.iter('dc_titles'))[0]
        child = etree.Element(name_tag_viaa)
        element.insert(0, child)
        child.text = self.general_info[tag_kp][0]

    def map_kp_general_to_viaa(self):
        """
        Reads the general DataFrame and maps this to an XML format
        :return: XML tags for <reeks>, <serie>, <seizoen>
        """
        self.ensure_element_exists('dc_titles')
        self.write_kp_general_to_dc_titles("serie", "name")
        self.write_kp_general_to_dc_titles("seizoen", "season")
        self.write_kp_general_to_dc_titles("reeks", "rerun")

    def ensure_element_exists(self, element_name):
        elements = self.tree.xpath('//' + element_name)
        if len(elements) == 0:
            element = list(self.tree.iter("MDProperties"))[0]
            child = etree.Element(element_name)
            element.insert(0, child)

    def map_kp_persons_to_viaa_makers(self, viaa_id):
        """
        Matches the functions from the kp persons dataframe to the viaa "Makers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_creators type="list">
        """

        element = list(self.tree.iter('dc_creators'))[0]

        for row in self.people_info.iterrows():
            full_name = row[1]["full name"]
            kp_function = row[1]["function"]
            viaa_function_level, viaa_function = self.map_kp_function_to_viaa_function(kp_function)
            if viaa_function_level == "Maker":
                if full_name == "Muriel Hérault":  # encoding problems!
                    pass
                else:
                    child = etree.Element(viaa_function)
                    element.insert(0, child)
                    child.text = full_name

    def map_kp_function_to_viaa_function(self, functie):
        with open(self.path_metadata_mapping, "r", "utf-8") as f:
            mapping_functies = load(f)
        for viaa_functie in mapping_functies["Maker"]:
            for kp_functie in mapping_functies["Maker"][viaa_functie]:
                if functie == kp_functie:
                    return "Maker", viaa_functie
                else:
                    pass
        for viaa_functie in mapping_functies["Bijdrager"]:
            for kp_functie in mapping_functies["Bijdrager"][viaa_functie]:
                if functie == kp_functie:
                    return "Bijdrager", viaa_functie
                else:
                    pass


    def map_kp_persons_to_viaa_contributors(self,viaa_id):
        """
        Matches the functions from the kp persons dataframe to the viaa "Bijdragers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_contributors type="list">
        """
        self.get_kp_metadata_general_for_viaa_id(viaa_id)
        self.get_kp_metadata_personen_for_viaa_id(viaa_id)
        with open(self.path_metadata_mapping, "r", "utf-8") as f:
            mapping_functies = load(f)
        try:
            (self.tree.find(".//dc_contributors").tag)
            for elements in self.tree.iter("dc_contributors"):
                for item in mapping_functies["Bijdrager"]:
                    for functie in mapping_functies["Bijdrager"][item]:
                        for i in range(len(self.people_info["full name"])):
                            if self.people_info["function"][i] == functie:
                                child = etree.Element(item)
                                elements.insert(0, child)
                                child.text = self.people_info["full name"][i]
        except:
            for elements in self.tree.iter("MDProperties"):
                child = etree.Element("dc_contributors")
                elements.insert(11, child)
            for elements in self.tree.iter("dc_contributors"):
                for item in mapping_functies["Bijdrager"]:
                    for functie in mapping_functies["Bijdrager"][item]:
                        for i in range(len(self.people_info["full name"])):
                            if self.people_info["function"][i] == functie:
                                child = etree.Element(item)
                                elements.insert(0, child)
                                child.text = self.people_info["full name"][i]
                                #print "<{0}>{1}</{0}>".format(item, self.people_info["full name"][i])



    def map_kp_organisations_to_viaa_makers(self, viaa_id):
        """
        Matches the functions from the kp organisations dataframe to the viaa "Makers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_creators type="list">
        """

        self.get_kp_metadata_general_for_viaa_id(viaa_id)
        self.get_kp_metadata_organisaties_for_viaa_id(viaa_id)
        with open(self.path_metadata_mapping, "r", "utf-8") as f:
            mapping_functies = load(f)
        for elements in self.tree.iter("dc_creators"):
            for item in mapping_functies["Maker"]:
                for functie in mapping_functies["Maker"][item]:
                    for i in range(len(self.organisations_info["organisatie"])):
                        if self.organisations_info["functie"][i] == functie:
                            #print "<{0}>{1}</{0}>".format(item, self.organisations_info["organisatie"][i])
                            child = etree.Element(item)
                            elements.insert(0, child)
                            child.text = self.organisations_info["organisatie"][i]


    def map_kp_organisations_to_viaa_contributors(self, viaa_id):
        """
        Matches the functions from the kp organisations dataframe to the viaa "Bijdragers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_contributors type="list">
        """
        self.get_kp_metadata_general_for_viaa_id(viaa_id)
        self.get_kp_metadata_organisaties_for_viaa_id(viaa_id)
        with open(self.path_metadata_mapping, "r", "utf-8") as f:
            mapping_functies = load(f)
        for elements in self.tree.iter("dc_contributors"):
            for item in mapping_functies["Bijdrager"]:
                for functie in mapping_functies["Bijdrager"][item]:
                    for i in range(len(self.organisations_info["organisatie"])):
                        if self.organisations_info["functie"][i] == functie:
                            #print "<{0}>{1}</{0}>".format(item, self.organisations_info["organisatie"][i])
                            child = etree.Element(item)
                            elements.insert(0, child)
                            child.text = self.organisations_info["organisatie"][i]


    def map_kp_genres_to_viaa_genres(self, viaa_id):
        """
        Matches the genres from the kp DataFrame to the viaa genres based on the mapping
        genres_mapping.json
        :param viaa_id:
        :return: XML tags for genres.
        """
        self.get_kp_metadata_general_for_viaa_id(viaa_id)
        self.get_kp_metadata_genres_for_viaa_id(viaa_id)
        with open("resources/genres_mapping.json", "r", "utf-8") as f:
            mapping_genres = load(f)
        try:
            (self.tree.find(".//genre").tag)
            for elements in self.tree.iter("genre"):
                for item in mapping_genres:
                    for genre in mapping_genres[item]:
                        for i in range(len(self.genre_info["Genre"])):
                            if self.genre_info["Genre"][i] == genre:
                                #print "<genre>{0}</genre>".format(self.genre_info["Genre"][i])
                                child = etree.Element(item)
                                elements.insert(0, child)
                                child.text = self.genre_info["Genre"][i]
        except:
            for elements in self.tree.iter("MDProperties"):
                child = etree.Element("genre")
                elements.insert(14, child)
            for elements in self.tree.iter("genre"):
                for item in mapping_genres:
                    for genre in mapping_genres[item]:
                        for i in range(len(self.genre_info["Genre"])):
                            if self.genre_info["Genre"][i] == genre:
                                #print "<genre>{0}</genre>".format(self.genre_info["Genre"][i])
                                child = etree.Element(item)
                                elements.insert(0, child)
                                child.text = self.genre_info["Genre"][i]

   # def map_kp_genres_to_viaa_genres_xml(self):






    def map_kp_language_to_viaa_language(self, viaa_id):
        """
        Matches the languages from the kp DataFrame to the viaa languages based on the mapping
        languages_mapping.json
        :param viaa_id:
        :return: XML tags for languages.
        """
        self.get_kp_metadata_general_for_viaa_id(viaa_id)
        self.get_kp_metadata_languages_for_viaa_id(viaa_id)
        with open("resources/languages_mapping.json", "r", "utf-8") as f:
            mapping_languages = load(f)
        for item in mapping_languages:
            for language in mapping_languages[item]:
                for i in range(len(self.language_info["taal"])):
                    if self.language_info["taal"][i] == language:
                        print "<multiselect>{0}</multiselect>".format(self.language_info["taal"][i])





    def map_kp_to_viaa(self,viaa_id):
        """
        Reads the Kunstenpunt metadata and appends this to an XML file
        :param viaa_id
        :return: XML file
        """

        self.tree.write("resources/test_file.XML", pretty_print=True, xml_declaration=True, encoding='UTF-8')

        #with open("resources/metadata_mapping.json", "r", "utf-8") as f:
        #    mapping = load(f)
        #print(dumps(mapping, indent=4))

        # self.map_kp_general_to_viaa(viaa_id)
        # print "<dc_creators type='list'>"
        # self.map_kp_persons_to_viaa_makers(viaa_id)
        # self.map_kp_organisations_to_viaa_makers(viaa_id)
        # print "</dc_creators>"
        # print "<dc_contributors type='list'>"
        # self.map_kp_persons_to_viaa_contributors(viaa_id)
        # self.map_kp_organisations_to_viaa_contributors(viaa_id)
        # print "</dc_contributors>"
        # print "<dc_types type='list'>"
        # self.map_kp_genres_to_viaa_genres(viaa_id)
        # print "</dc_types>"




if __name__ == "__main__":
    kp2viaa = KP2VIAA()
    kp2viaa.read_mapping_viaa_to_kp()
    kp2viaa.read_viaa_xml_to_tree()
    kp2viaa.map_kp_to_viaa("viaa_id")
    #kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
    #kp2viaa.map_kp_to_viaa("viaa_id")
    kp2viaa.map_kp_general_to_viaa("viaa_id")
    kp2viaa.map_kp_persons_to_viaa_makers("viaa_id")
    kp2viaa.map_kp_persons_to_viaa_contributors("viaa_id")
    kp2viaa.map_kp_organisations_to_viaa_contributors("viaa_id")
    kp2viaa.map_kp_organisations_to_viaa_makers("viaa_id")
    kp2viaa.map_kp_genres_to_viaa_genres("viaa_id")
    kp2viaa.map_kp_to_viaa("viaa_id")


