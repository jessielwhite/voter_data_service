from google.cloud import bigquery

class Table(bigquery.Table):
    def __init__(self, *args, **kwargs):
       super(*args, **kwargs)

    def set_primary_key(self, primary_key):
        if 'tableConstraints' not in self._properties:
            self._properties['tableConstraints'] = {}
        self._properties['tableConstraints']['primaryKey'] = primary_key
    def set_foreign(self, foreign_key):
        if 'tableConstraints' not in self._properties:
            self._properties['tableConstraints'] = {}
        self._properties['tableConstraints']['foreignKey'] = foreign_key

def create_voter_registry_table(table_id):
    """
    Establishes the schema for voter_registry table.
    """
    schema = [
        bigquery.SchemaField("sos_voter_id", "INTEGER", mode="REQUIRED", description="Secretary of State unique voter ID (Primary Key)"),
        bigquery.SchemaField("last_name", "STRING", description="Voter last name"),
        bigquery.SchemaField("first_name", "STRING", description="Voter first name"),
        bigquery.SchemaField("middle_name", "STRING", description="Voter middle name"),
        bigquery.SchemaField("suffix", "STRING", description="Voter name suffix"),
        bigquery.SchemaField("date_of_birth", "DATE", description="Voter Date of Birth (YYYY-MM-DD)"),
        bigquery.SchemaField("registration_date", "DATE", description="Date voter registered (YYYY-MM-DD)"),
        bigquery.SchemaField("voter_status", "STRING", description="Current Voter Status"),
        bigquery.SchemaField("party_affiliation", "STRING", description="Party affiliation from last Primary Election"),
        bigquery.SchemaField("residential_address1", "STRING", description="Voter street address"),
        bigquery.SchemaField("residential_address2", "STRING", description="Voter street address (continued)"),
        bigquery.SchemaField("residential_city", "STRING", description="Voter city"),
        bigquery.SchemaField("residential_state", "STRING", description="Voter state"),
        bigquery.SchemaField("residential_zip", "STRING", description="Voter ZIP code"),
        bigquery.SchemaField("residential_zip_plus_4", "STRING", description="Voter ZIP+4"),
        bigquery.SchemaField("residential_country", "STRING", description="Voter country"),
        bigquery.SchemaField("residential_postal_code", "STRING", description="Voter postal code"),
        bigquery.SchemaField("mailing_address1", "STRING", description="Voter mailing address"),
        bigquery.SchemaField("mailing_address2", "STRING", description="Voter mailing address (continued)"),
        bigquery.SchemaField("mailing_city", "STRING", description="Mailing city"),
        bigquery.SchemaField("mailing_state", "STRING", description="Mailing state"),
        bigquery.SchemaField("mailing_zip", "STRING", description="Mailing ZIP code"),
        bigquery.SchemaField("mailing_zip_plus_4", "STRING", description="Mailing ZIP+4"),
        bigquery.SchemaField("mailing_country", "STRING", description="Mailing country"),
        bigquery.SchemaField("mailing_postal_code", "STRING", description="Mailing postal code"),
        bigquery.SchemaField("county_number", "STRING", description="County number (01-88)"),
        bigquery.SchemaField("county_id", "STRING", description="County system generated unique number"),
        bigquery.SchemaField("career_center", "STRING", description="Name of Career Center"),
        bigquery.SchemaField("city", "STRING", description="Name of City"),
        bigquery.SchemaField("city_school_district", "STRING", description="Name of City School District"),
        bigquery.SchemaField("county_court_district", "STRING", description="Name of County Court District"),
        bigquery.SchemaField("congressional_district", "STRING", description="Congressional District identifier"),
        bigquery.SchemaField("court_of_appeals", "STRING", description="Court of Appeals identifier"),
        bigquery.SchemaField("education_service_center", "STRING", description="Name of Education Service Center"),
        bigquery.SchemaField("exempted_village_school_district", "STRING", description="Name of Exempted Village School District"),
        bigquery.SchemaField("library_district", "STRING", description="Name of Library District"),
        bigquery.SchemaField("local_school_district", "STRING", description="Name of Local School District"),
        bigquery.SchemaField("municipal_court_district", "STRING", description="Name of Municipal Court District"),
        bigquery.SchemaField("precinct", "STRING", description="Name of Precinct"),
        bigquery.SchemaField("precinct_code", "STRING", description="Code assigned to the Precinct"),
        bigquery.SchemaField("state_board_of_education", "STRING", description="State Board of Education identifier"),
        bigquery.SchemaField("state_representative_district", "STRING", description="House District identifier"),
        bigquery.SchemaField("state_senate_district", "STRING", description="State Senate District identifier"),
        bigquery.SchemaField("township", "STRING", description="Name of Township"),
        bigquery.SchemaField("village", "STRING", description="Name of Village"),
        bigquery.SchemaField("ward", "STRING", description="Name of Ward"),
    ]

    primary_key={"columns": ["sos_voter_id"]}
    table = Table(table_id, schema=schema)
    table.set_primary_key(primary_key)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DATE,
        field="registration_date"
    )
    table.clustering_fields = [
        "residential_address_1",
        "residential_address_2",
        "voter_status",
        "county",
        "party_affiliation"
    ]

    client = bigquery.Client('PROJECT_ID')
    table = client.create_table(table)

table_id = "TABLE_ID"

create_voter_registry_table(table_id)

def create_voting_history_table(table_id):
    """Establishes the schema for voting_history table."""
    schema = [
        bigquery.SchemaField(
            "sos_voter_id",
            "INTEGER",
            mode="REQUIRED",
            description="Secretary of State unique voter ID (Foreign Key referencing voter_registry.sos_voter_id)",
            policy_tags=None,
            fields=(),
            default_value_expression=None
        ),
        bigquery.SchemaField("election_type", "STRING", description="PRIMARY, SPECIAL, or GENERAL"),
        bigquery.SchemaField("election_date", "DATE", description="Date of election"),
        bigquery.SchemaField(
            "vote_cast", "STRING", description="Single char value representing party vote was cast for. Can also be empty."
        ),
    ]

    foreign_key={"columns": ["sos_voter_id"]}
    table = Table(table_id, schema=schema)
    table.set_foreign_key(foreign_key)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DATE,
        field="election_date"
    )

    client = bigquery.Client('PROJECT_ID')
    table = client.create_table(table)

table_id = "TABLE_ID"
create_voting_history_table(table_id)

def hash_columns_for_search(fields_to_hash):
    """
    The hash function takes an arbitrary array of columns from the voter_registry table and generates an identifiable hash of the values. 
    In this example the hash is generated for a combination of Party Affiliation and Zip Code.
    """
    fields_to_hash = []
    search_exprs = []
    for field in fields_to_hash:
        search_exprs.append(f"TO_HEX(SHA256(CAST({field} AS STRING)))")
    return ", ".join(search_exprs)

fields_to_hash = ["residential_zip", "party_affiliation"]

hash_columns_for_search(fields_to_hash)





