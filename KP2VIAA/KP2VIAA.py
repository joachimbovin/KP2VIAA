class KP2VIAA(object):
    def __init__(self):
        self.mapping = None

    def read_mapping_VIAA_to_KP(self):
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
        self.mapping = {
            "viaa_id": {
                "kp_productie_id": 449364,
                "kp_show_id": "kp_show_id"
            }
        }

    def get_KP_metadata_for_VIAA_id(self, viaa_id):
        """
        Returns the kunstenpunt metadata for a specific viaa_id. The viaa id is mapped to a kunstenpunt production id
        and a kunstenpunt show id
        :param viaa_id: the original viaa id
        :return: a pandas object containing all the kunstenpunt metadata for the production and potentially the show
        """
        productie_id = self.mapping[viaa_id]["kp_productie_id"]
        sql = """
        SELECT * FROM production.productions WHERE productions.id == {0}
        """.format(productie_id)
        return sql


if __name__ == "__main__":
    kp2viaa = KP2VIAA()
    kp2viaa.read_mapping_VIAA_to_KP()
    print(kp2viaa.get_KP_metadata_for_VIAA_id("viaa_id"))
