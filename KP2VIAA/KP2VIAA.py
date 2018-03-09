from json import load, dumps
from codecs import open
import psycopg2
from configparser import ConfigParser
from pandas import DataFrame


class KP2VIAA(object):
    def __init__(self, path_to_dbcfg="resources/db.cfg"):
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
        self.viaa_id_to_kp_productie_show_id_mapping = {
            "viaa_id": {
                "kp_productie_id": 429013,
                "kp_show_id": "kp_show_id"
            }
        }

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

    def get_kp_metadata_functies_for_viaa_id(self, viaa_id):
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

    def map_kp_to_viaa(self):
        """
        Reads the Kunstenpunt metadata and appends this to an XML file
        :return: XML file
        """
        with open("resources/metadata_mapping.json", "r", "utf-8") as f:
            mapping = load(f)
        print(dumps(mapping, indent=4))



if __name__ == "__main__":
    kp2viaa = KP2VIAA()
    kp2viaa.read_mapping_viaa_to_kp()
    #print(kp2viaa.get_KP_metadata_for_VIAA_id("viaa_id"))
    print(kp2viaa.get_kp_metadata_functies_for_viaa_id("viaa_id"))
    #print(kp2viaa.get_KP_metadata_organisaties_for_VIAA_id("viaa_id"))
    kp2viaa.map_kp_to_viaa()
