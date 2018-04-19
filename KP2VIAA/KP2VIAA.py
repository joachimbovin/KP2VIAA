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
                 path_to_viaa2kp="resources/viaa2kp_id.json",
                 path_metadata_mapping="resources/metadata_mapping.json",
                 path_genres_mapping="resources/genres_mapping.json",
                 path_languages_mapping="resources/languages_mapping.json",
                 path_to_qas_auth="resources/qasviaaauthenticationbase64.txt",
                 path_to_xsd = "resources/viaa_metadatamodel_van_viaa_naar_mam.xsd",
                 path_to_pass_viaa = "resources/pass_viaa.txt"):
        self.path_to_viaa2kp_mapping = path_to_viaa2kp
        self.path_metadata_mapping = path_metadata_mapping
        self.path_genres_mapping = path_genres_mapping
        self.path_languages_mapping = path_languages_mapping
        self.path_to_qas_auth = path_to_qas_auth
        self.path_to_pass_viaa = path_to_pass_viaa
        self.viaa_id_to_kp_productie_show_id_mapping = None
        self.path_to_dbcfg = path_to_dbcfg
        self.path_to_xsd = path_to_xsd
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
        self.update_tree = None

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
        return cur

    def get_kp_metadata_general_for_viaa_id(self, viaa_id):
        """
        Returns the kunstenpunt metadata about the production for a specific viaa_id. The viaa id is mapped to a kunstenpunt production id
        and a kunstenpunt show id.
        :param viaa_id: the original viaa id
        :return: A pandas object containing all the kunstenpunt metadata for the production and potentially the show.
        """
        cur = self.get_access_database()
        kp_productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT pr.title, seasons.name, rerun.title, seasons_2.name
        FROM production.productions AS pr
        JOIN production.seasons AS seasons
        ON pr.season_id = seasons.id
        LEFT JOIN production.productions AS rerun
        ON pr.rerun_of_id = rerun.id
        LEFT JOIN production.seasons AS seasons_2
        ON rerun.season_id = seasons_2.id
        WHERE pr.id={0}
        """.format(kp_productie_id)
        cur.execute(sql)
        general_info = cur.fetchall()
        self.general_info = DataFrame(general_info, columns=['name', 'season', 'rerun_title','rerun_season'])
        if self.general_info["rerun_title"][0] == None:
            self.general_info = self.general_info.drop(['rerun_title', 'rerun_season'], axis=1)
            self.general_info['rerun'] = None
        else:
            self.general_info['rerun'] = self.general_info[['rerun_title', 'rerun_season']].apply(lambda x: ' '.join(x), axis=1)
            self.general_info = self.general_info.drop(['rerun_title', 'rerun_season'], axis=1)

    def get_kp_metadata_personen_for_viaa_id(self, viaa_id):
        """
        Returns the KP metadata about the persons and their function based on the VIAA ID.
        :param viaa_id:
        :return: A pandas object containing all the kunstenpunt metadata for the persons and their functions.
        """
        cur = self.get_access_database()
        kp_productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
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
          """.format(kp_productie_id)
        cur.execute(sql)
        people_info = cur.fetchall()
        self.people_info = DataFrame(people_info, columns=['full name', 'function', 'production id'])

    def get_kp_metadata_organisaties_for_viaa_id(self, viaa_id):
        """
        Returns the KP metadata about the organisations and their function based on the VIAA ID.
        :param v
        iaa_id:
        :return:
        """
        cur = self.get_access_database()
        kp_productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
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
        """.format(kp_productie_id)
        cur.execute(sql)
        organisations_info = cur.fetchall()
        self.organisations_info = DataFrame(organisations_info, columns=['organisation', 'function'])

    def get_kp_metadata_genres_for_viaa_id(self, viaa_id):
        """
        Return the KP metadata about the genre based on the VIAA ID
        :param viaa_id:
        :return:
        """
        cur = self.get_access_database()
        kp_productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT pr.title, genres.name_nl
        FROM production.productions AS pr
        JOIN production.relationships AS prod_rel_genre
        ON pr.id = prod_rel_genre.production_id
        JOIN production.genres AS genres
        ON prod_rel_genre.genre_id = genres.id
        WHERE pr.id={0}    
        """.format(kp_productie_id)
        cur.execute(sql)
        genres_info = cur.fetchall()
        self.genre_info = DataFrame(genres_info, columns=['show','genre'])

    def get_kp_metadata_languages_for_viaa_id(self, viaa_id):
        """
        Creates class variable containing DataFrame with kp metadata about languages based on the viaa id
        :param viaa_id:
        :return: metadata languages
        """
        cur = self.get_access_database()
        kp_productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql = """SELECT pr.title, lang.name_nl
        FROM production.productions AS pr
        LEFT JOIN production.production_languages AS prod_lang
        ON prod_lang.production_id = pr.id
        LEFT JOIN production.languages as lang
        ON prod_lang.language_id = lang.id
        WHERE pr.id={0}
        """.format(kp_productie_id)
        cur.execute(sql)
        language_info = cur.fetchall()
        self.language_info = DataFrame(language_info, columns=['show','language'])

    def consume_api(self, viaa_id):
        with open(self.path_to_qas_auth, "r") as f:
            base64pass = f.read()
        header = {
            "Accept": "application/xml",
            "Authorization": "Basic " + base64pass
        }
        #url = "https://archief-qas.viaa.be/mediahaven-rest-api/resources/media/{0}".format(viaa_id)
        url = "https://archief-qas.viaa.be/mediahaven-rest-api/resources/media/?q=%2B(MediaObjectFragmentPID:{0})".format(viaa_id)
        r = requests.get(url, headers=header)
        parser = etree.XMLParser(ns_clean=True, recover=True, encoding="utf-8")
        self.mediahaven_xml = etree.fromstring(r.text.encode("utf-8"), parser=parser)

    def create_viaa_xml(self):
        """
        Reads the viaa xml file to a xml tree
        :return: class variable xml tree
        """
        self.update_tree = etree.Element("MediaHAVEN_external_metadata")
        self.update_tree.append(etree.Element("MDProperties"))


    def validate_xml_viaa_xsd(self):
        """
        Create a number of elements and add to update_tree to validate based on viaa xsd + insert
        tags with specific content on specific information
        :return:  /
        """
        tags = ["CP", "CP_id", "PID", "dc_source", "dc_relations", "dc_identifier_localid", "dc_identifier_localids", "dc_title",
               "dc_titles", "dcterms_issued","dcterms_created", "dc_creators", "dc_contributors", "dc_publishers", "dc_subjects", "dc_types",
               "dc_coverages", "dc_languages", "dc_rights_licenses", "dc_rights_rightsOwners", "dc_rights_rightsHolders"]
        for item in tags:
           self.insert_tags_xml("MDProperties", item, tags.index(item))

        contentnav = self.update_tree.find(".//dc_identifier_localids")
        child = etree.Element("md5")
        contentnav.addnext(child)
        child.text = "73fc87dc8bce8cdc244d2dc95ad576ff"
        contentnav_2 = self.update_tree.find(".//dcterms_created")
        child = etree.Element("CreationDate")
        contentnav_2.addnext(child)
        child.text = "1992:08:21 00:00:00"


    def insert_tags_xml(self, parent_tag, child_tag, index):
        element = list(self.update_tree.iter(parent_tag))[0]
        child = etree.Element(child_tag)
        element.insert(index, child)

    def ensure_element_exists(self, element_name):
        elements = self.update_tree.xpath('//' + element_name)
        if len(elements) == 0:
            element = list(self.update_tree.iter("MDProperties"))[0]
            child = etree.Element(element_name)
            element.insert(0, child)

    def map_kp_general_to_dc_titles(self, name_tag_viaa, tag_kp):
        """
        Looks for element dc_titles in update_tree and appends elements based on parameters viaa tag name and info from DataFrame
        general_info
        :param name_tag_viaa:
        :param tag_kp:
        :return:
        """
        element = list(self.update_tree.iter('dc_titles'))[0]
        child = etree.Element(name_tag_viaa)
        element.insert(0, child)
        child.text = self.general_info[tag_kp][0]

    def write_kp_general_to_update_tree(self):
        """
        Reads the general DataFrame and writes this to an XML tree
        :return: XML tags for <reeks>, <serie>, <seizoen>
        """
        self.ensure_element_exists('dc_titles')
        self.map_kp_general_to_dc_titles("serie", "name")
        self.map_kp_general_to_dc_titles("seizoen", "season")
        if self.general_info["rerun"][0] is None:
            pass
        else:
            self.map_kp_general_to_dc_titles("reeks", "rerun")

    def map_kp_function_to_viaa_function(self, functie):
        """
        Maps a kp function to the corresponding viaa function using metadata_mapping.json
        :param functie:
        :return: 2 variables = "Maker" or Bijdrager AND viaa_functie
        """
        with open(self.path_metadata_mapping, "r", "utf-8") as f:
            mapping_functies = load(f)
        for viaa_functie in mapping_functies["Maker"]:
            for kp_functie in mapping_functies["Maker"][viaa_functie]:
                if functie.decode("utf-8") == kp_functie:
                    return "Maker", viaa_functie
                else:
                    pass
        for viaa_functie in mapping_functies["Bijdrager"]:
            for kp_functie in mapping_functies["Bijdrager"][viaa_functie]:
                if functie.decode("utf-8") == kp_functie:
                    return "Bijdrager", viaa_functie
                else:
                    pass

    def write_kp_persons_to_viaa_makers(self):
        """
        Matches the functions from the kp persons dataframe to the viaa "Makers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_creators type="list">
        """
        element = list(self.update_tree.iter('dc_creators'))[0]

        for row in self.people_info.iterrows():
            full_name = row[1]["full name"]
            kp_function = row[1]["function"]
            viaa_function_level, viaa_function = self.map_kp_function_to_viaa_function(kp_function)
            is_in_mediahaven = self.compare_mediahaven_kunstenpunt(full_name, viaa_function)
            if viaa_function_level == "Maker" and is_in_mediahaven is False:
                    child = etree.Element(viaa_function)
                    element.insert(0, child)
                    child.text = full_name.decode("utf-8")
            else:
                pass

    def write_kp_persons_to_viaa_contributors(self):
        """
        Matches the functions from the kp persons dataframe to the viaa "Bijdragers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_contributors type="list">
        """

        element = list(self.update_tree.iter('dc_contributors'))[0]

        for row in self.people_info.iterrows():
            full_name = row[1]["full name"]
            kp_function = row[1]["function"]
            viaa_function_level, viaa_function = self.map_kp_function_to_viaa_function(kp_function)
            is_in_mediahaven = self.compare_mediahaven_kunstenpunt(full_name, viaa_function)
            if viaa_function_level == "Bijdrager" and is_in_mediahaven is False:
                    child = etree.Element(viaa_function)
                    element.insert(0, child)
                    child.text = full_name.decode("utf-8")
            else:
                pass

    def write_kp_organisations_to_viaa_makers(self):
        """
        Matches the functions from the kp organisations dataframe to the viaa "Makers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_creators type="list">
        """

        element = list(self.update_tree.iter('dc_creators'))[0]

        for row in self.organisations_info.iterrows():
            full_name = row[1]["organisation"]
            kp_function = row[1]["function"]
            viaa_function_level, viaa_function = self.map_kp_function_to_viaa_function(kp_function)
            is_in_mediahaven = self.is_in_mediahaven(full_name, viaa_function)
            if viaa_function_level == "Maker" and is_in_mediahaven is False:
                    child = etree.Element(viaa_function)
                    element.insert(0, child)
                    child.text = full_name.decode("utf-8")
            else:
                pass

    def write_kp_organisations_to_viaa_contributors(self):
        """
        Matches the functions from the kp organisations dataframe to the viaa "Bijdragers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_contributors type="list">
        """

        element = list(self.update_tree.iter('dc_contributors'))[0]

        for row in self.organisations_info.iterrows():
            full_name = row[1]["organisation"]
            kp_function = row[1]["function"]
            viaa_function_level, viaa_function = self.map_kp_function_to_viaa_function(kp_function)
            is_in_mediahaven = self.compare_mediahaven_kunstenpunt(full_name, viaa_function)
            if viaa_function_level == "Bijdrager" and is_in_mediahaven is False:
                    child = etree.Element(viaa_function)
                    element.insert(0, child)
                    child.text = full_name.decode("utf-8")
            else:
                pass

    def map_kp_genres_to_viaa_genres(self, genre):
        """
        Maps the kp genre to the viaa genre metadatamodel based on genres_mapping.json
        :param genre: the kp genre
        :return: viaa genre
        """
        with open(self.path_genres_mapping, "r", "utf-8") as f:
            mapping_genres = load(f)
        for viaa_genre in mapping_genres:
            for kp_genre in mapping_genres[viaa_genre]:
                if genre == kp_genre:
                    return viaa_genre
                else:
                    pass

    def write_kp_genres_to_viaa_genres(self):
        """
        Writes the genres metadata to XML based on mapping
        :param viaa_id:
        :return: XML tags for genres.
        """
        element = list(self.update_tree.iter('dc_types'))[0]

        for row in self.genre_info.iterrows():
            kp_genre = row[1]["genre"]
            viaa_genre = self.map_kp_genres_to_viaa_genres(kp_genre)
            child = etree.Element("multiselect")
            element.insert(0, child)
            child.text = viaa_genre.decode("utf-8")    #?!

    def map_kp_languages_to_viaa_languages(self, language):
        """
        Maps the kp language to the viaa language metadatamodel based on languages_mapping.json
        :param genre: the kp language
        :return: viaa language
        """
        with open(self.path_languages_mapping, "r", "utf-8") as f:
            mapping_languages = load(f)
        for viaa_language in mapping_languages:
            for kp_language in mapping_languages[viaa_language]:
                if language == kp_language:
                    return viaa_language
                else:
                    pass

    def write_kp_languages_to_viaa_languages(self):
        """
        writes the languages from the kp DataFrame to the viaa XML based on the mapping
        languages_mapping.json
        :param viaa_id:
        :return: XML tags for languages. <dc_languages> <multiselect> nl </multiselect> </dc_languages>
        """

        element = list(self.update_tree.iter('dc_languages'))[0]

        if self.language_info["language"][0] is None:
            pass
        else:
            for row in self.language_info.iterrows():
                kp_taal = row[1]["language"]
                viaa_language = self.map_kp_languages_to_viaa_languages(kp_taal)
                child = etree.Element("multiselect")
                element.insert(0, child)
                child.text = viaa_language.decode("utf-8")    #?!  why not [0]?


    def check_doubles(self, viaa_name, viaa_function):
        #name = self.mediahaven_xml.xpath(u'.//value[text()="' + viaa_name.decode("utf-8") + u'"]').getnext()
        #print name.text

        element = list(self.mediahaven_xml.iter('mdProperties'))[0]



    def is_in_mediahaven(self, viaa_name, viaa_function): # TODO change this method so that it specifically searches for a name and the next
        #element (=the function), now it just searches the entire document!!!
        """
        Searches the VIAA XML for name and function based on parameters and sends this to
        compare_mediahaven_kunstenpunt
        :param viaa_name:
        :param viaa_function:
        :return: ...
        """
        name = self.mediahaven_xml.xpath(u'.//value[text()="' + viaa_name.decode("utf-8") + u'"]')
        function = self.mediahaven_xml.xpath(u'.//key[text()="' + viaa_function.decode("utf-8") + u'"]')
        if len(name) == 0 and len(function) == 0:
            return None, None
        elif len(name) != 0 and len(function) == 0:
            return name[0].text, None
        elif len(name) == 0 and len(function) != 0:
            return None, function[0].text
        else:
            return name[0].text, function[0].text

    def compare_mediahaven_kunstenpunt(self, viaa_name, viaa_function):
        mediahaven_name, mediahaven_function = self.is_in_mediahaven(viaa_name, viaa_function)
        if mediahaven_name is None:
            return False
        elif mediahaven_name == viaa_name and mediahaven_function == viaa_function:
            return True
        else:
            return False

    def test_if_PID_unique(self): #!!
        if int(self.mediahaven_xml[0].text) == 1:
            print "OK!"
        else:
            print "Error!" # raise user generated error "multiple items found in viaa for pid" (https://docs.python.org/2/tutorial/errors.html#user-defined-exceptions)

    def get_title_mediahaven(self):
        """
        Gets title from mediahaven xml and adds to update_tree, do not use!
        :return:
        """
        element = list(self.update_tree.iter('MediaHAVEN_external_metadata'))[0]
        title = list(self.mediahaven_xml.xpath('//title'))[0]
        child = etree.Element("title")
        element.insert(0, child)
        child.text = title.text

    def get_mediahaven_fragmentId(self):
        """
        Gets fragementID from mediahaven XML
        :return:
        """
        fragmentId = list(self.mediahaven_xml.xpath('//fragmentId'))[0]
        return fragmentId.text

    def validate_updated_tree_to_VIAA_xsd(self):

        viaa_xmlschema_doc = etree.parse(self.path_to_xsd)
        viaa_xmlschema = etree.XMLSchema(viaa_xmlschema_doc)
        viaa_xmlschema.validate(self.update_tree)
        log = viaa_xmlschema.error_log
        #error = log.last_error
        print log

    def write_tree_to_xml(self):
        """
        writes the the xlm tree to an xml file
        :return:
        """

        #payload = etree.tostring(self.update_tree, pretty_print=True, xml_declaration=True, encoding='UTF-8')
        with open("../resources/xml_viaa.xml", "w") as f:
            f.write(etree.tostring(self.update_tree, pretty_print=True, xml_declaration=True, encoding='UTF-8'))

    def send_update_tree_to_viaa_new(self):

        with open(self.path_to_pass_viaa, "r") as f:
            pass_viaa = f.read()
        fragmentId = self.get_mediahaven_fragmentId()
        url = "https://archief-qas.viaa.be/mediahaven-rest-api/resources/media/{0}".format(fragmentId)
        username = "joachim.bovin@student.kuleuven.be"
        passwd = pass_viaa
        files = {'metadata': ('../resources/test.xml', open('../resources/test.xml', 'rb'))}
        res = requests.post(url, files=files, auth=(username, passwd))
        print res


if __name__ == "__main__":
    kp2viaa = KP2VIAA()
    kp2viaa.read_mapping_viaa_to_kp()
    kp2viaa.create_viaa_xml()
    kp2viaa.send_update_tree_to_viaa("viaa_id")
    #kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
    #kp2viaa.map_kp_to_viaa("viaa_id")
    kp2viaa.write_kp_general_to_update_tree("viaa_id")
    kp2viaa.write_kp_persons_to_viaa_makers("viaa_id")
    kp2viaa.write_kp_persons_to_viaa_contributors("viaa_id")
    kp2viaa.write_kp_organisations_to_viaa_contributors("viaa_id")
    kp2viaa.write_kp_organisations_to_viaa_makers("viaa_id")
    kp2viaa.map_kp_genres_to_viaa_genres("viaa_id")
    kp2viaa.send_update_tree_to_viaa("viaa_id")


