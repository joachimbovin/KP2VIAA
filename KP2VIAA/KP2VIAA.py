from json import load, dumps
from codecs import open
import psycopg2
from configparser import ConfigParser
from pandas import DataFrame
from lxml import etree


class KP2VIAA(object):
    def __init__(self, path_to_dbcfg="resources/db.cfg", path_to_viaa2kp="resources/viaa_id_testcase.json"):
        self.path_to_viaa2kp_mapping = path_to_viaa2kp
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

    def get_kp_metadata_for_viaa_id(self, viaa_id):
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
        sql2 = """
        SELECT people.first_name, people.name, fun.name_nl, pr.id
        FROM production.productions AS pr
        JOIN production.relationships AS rel
        ON pr.id = rel.production_id
        JOIN production.people as people
        ON rel.person_id = people.id
        JOIN production.functions AS fun
        ON rel.function_id = fun.id
        WHERE pr.id={0}
          """.format(productie_id)
        cur.execute(sql2)
        rosas_productions = cur.fetchall()
        self.people_info = DataFrame(rosas_productions, columns=['first name', 'name', 'function', 'production id'])
        self.people_info["full name"] = self.people_info[['first name', 'name']].apply(lambda x: ' '.join(x), axis=1)
        self.people_info.drop(['first name', 'name'], axis=1, inplace=True)

    def get_kp_metadata_organisaties_for_viaa_id(self, viaa_id):
        """
        Returns the KP metadata about the organisations and their function based on the VIAA ID.
        :param viaa_id:
        :return:
        """
        cur = self.get_access_database()
        productie_id = self.viaa_id_to_kp_productie_show_id_mapping[viaa_id]["kp_productie_id"]
        sql3 = """
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
        cur.execute(sql3)
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
        sql4 = """
        SELECT pr.title, genres.name_nl
        FROM production.productions AS pr
        JOIN production.relationships AS prod_rel_genre
        ON pr.id = prod_rel_genre.production_id
        JOIN production.genres AS genres
        ON prod_rel_genre.genre_id = genres.id
        WHERE pr.id={0}    
        """.format(productie_id)
        cur.execute(sql4)
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
        sql5 = """SELECT pr.title, lang.name_nl
        FROM production.productions AS pr
        LEFT JOIN production.production_languages AS prod_lang
        ON prod_lang.production_id = pr.id
        LEFT JOIN production.languages as lang
        ON prod_lang.language_id = lang.id
        WHERE pr.id={0}
        """.format(productie_id)
        cur.execute(sql5)
        rosas_productions = cur.fetchall()
        self.language_info = DataFrame(rosas_productions, columns=['voorstelling','taal'])


    def read_viaa_xml_to_tree(self):
        parser = etree.XMLParser(remove_blank_text=True)
        with open("resources/viaa_xml_testcase.xml") as file:   #change this to parameter xml_viaa
            tree = etree.parse(file, parser)
            self.tree = tree

    def map_kp_general_to_viaa(self, viaa_id):
        """
        Reads the general DataFrame and maps this to an XML format
        :return: XML tags for <reeks>, <serie>, <seizoen>
        """
        self.get_kp_metadata_for_viaa_id(viaa_id)
        general_info = self.general_info
        root = self.tree.getroot()
        try:
            (self.tree.find(".//dc_titles").tag)
            for elements in self.tree.iter('dc_titles'):
                child = etree.Element("reeks")
                elements.insert(0, child)
                child.text = general_info["rerun"][0]
                child_2 = etree.Element("serie")
                elements.insert(0, child_2)
                child_2.text = general_info["name"][0]
                child_3 = etree.Element("seizoen")
                elements.insert(0, child_3)
                child_3.text = general_info["season"][0]
        except:
            for elements in self.tree.iter("MDProperties"):
                child = etree.Element("dc_titles")
                elements.insert(11, child)
            for elements in self.tree.iter('dc_titles'):
                child = etree.Element("reeks")
                elements.insert(0, child)
                child.text = general_info["rerun"][0]
                child_2 = etree.Element("serie")
                elements.insert(0, child_2)
                child_2.text = general_info["name"][0]
                child_3 = etree.Element("seizoen")
                elements.insert(0, child_3)
                child_3.text = general_info["season"][0]



    def map_kp_persons_to_viaa_makers(self,viaa_id):
        """
        Matches the functions from the kp persons dataframe to the viaa "Makers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_creators type="list">
        """
        self.get_kp_metadata_for_viaa_id(viaa_id)
        self.get_kp_metadata_personen_for_viaa_id(viaa_id)
        with open("resources/metadata_mapping.json", "r", "utf-8") as f:
            mapping_functies = load(f)


        for item in mapping_functies["Maker"]:
            for functie in mapping_functies["Maker"][item]:
                for i in range(len(self.people_info["full name"])):
                    if self.people_info["function"][i] == functie:
                        print "<{0}>{1}</{0}>".format(item, self.people_info["full name"][i])


    def map_kp_persons_to_viaa_contributors(self,viaa_id):
        """
        Matches the functions from the kp persons dataframe to the viaa "Bijdragers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_contributors type="list">
        """
        self.get_kp_metadata_for_viaa_id(viaa_id)
        self.get_kp_metadata_personen_for_viaa_id(viaa_id)
        with open("resources/metadata_mapping.json", "r", "utf-8") as f:
            mapping_functies = load(f)
        for item in mapping_functies["Bijdrager"]:
            for functie in mapping_functies["Bijdrager"][item]:
                for i in range(len(self.people_info["full name"])):
                    if self.people_info["function"][i] == functie:
                        print "<{0}>{1}</{0}>".format(item, self.people_info["full name"][i])


    def map_kp_organisations_to_viaa_makers(self, viaa_id):
        """
        Matches the functions from the kp organisations dataframe to the viaa "Makers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_creators type="list">
        """

        self.get_kp_metadata_for_viaa_id(viaa_id)
        self.get_kp_metadata_organisaties_for_viaa_id(viaa_id)
        with open("resources/metadata_mapping.json", "r", "utf-8") as f:
            mapping_functies = load(f)
        for item in mapping_functies["Maker"]:
            for functie in mapping_functies["Maker"][item]:
                for i in range(len(self.organisations_info["organisatie"])):
                    if self.organisations_info["functie"][i] == functie:
                        print "<{0}>{1}</{0}>".format(item, self.organisations_info["organisatie"][i])

    def map_kp_organisations_to_viaa_contributors(self, viaa_id):
        """
        Matches the functions from the kp organisations dataframe to the viaa "Bijdragers" functions based on the mapping
        from the metadata_mapping.json
        :param viaa_id:
        :return: XML tags for <dc_contributors type="list">
        """
        self.get_kp_metadata_for_viaa_id(viaa_id)
        self.get_kp_metadata_organisaties_for_viaa_id(viaa_id)
        with open("resources/metadata_mapping.json", "r", "utf-8") as f:
            mapping_functies = load(f)
        for item in mapping_functies["Bijdrager"]:
            for functie in mapping_functies["Bijdrager"][item]:
                for i in range(len(self.organisations_info["organisatie"])):
                    if self.organisations_info["functie"][i] == functie:
                        print "<{0}>{1}</{0}>".format(item, self.organisations_info["organisatie"][i])

    def map_kp_genres_to_viaa_genres(self, viaa_id):
        """
        Matches the genres from the kp DataFrame to the viaa genres based on the mapping
        genres_mapping.json
        :param viaa_id:
        :return: XML tags for genres.
        """
        self.get_kp_metadata_for_viaa_id(viaa_id)
        self.get_kp_metadata_genres_for_viaa_id(viaa_id)
        with open("resources/genres_mapping.json", "r", "utf-8") as f:
            mapping_genres = load(f)
        for item in mapping_genres:
            for genre in mapping_genres[item]:
                for i in range(len(self.genre_info["Genre"])):
                    if self.genre_info["Genre"][i] == genre:
                        print "<genre>{0}</genre>".format(self.genre_info["Genre"][i])


    def map_kp_language_to_viaa_language(self, viaa_id):
        """
        Matches the languages from the kp DataFrame to the viaa languages based on the mapping
        languages_mapping.json
        :param viaa_id:
        :return: XML tags for languages.
        """
        self.get_kp_metadata_for_viaa_id(viaa_id)
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

        self.tree.write("resources/viaa_xml_testcase.XML", pretty_print=True, xml_declaration=True)

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
    #kp2viaa.map_kp_to_viaa("viaa_id")
    #kp2viaa.get_kp_metadata_genres_for_viaa_id("viaa_id")
    #kp2viaa.map_kp_to_viaa("viaa_id")
    kp2viaa.read_viaa_xml_to_tree()
    kp2viaa.map_kp_general_to_viaa("viaa_id")
    kp2viaa.map_kp_to_viaa("viaa_id")


