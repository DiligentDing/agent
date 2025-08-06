import pymysql
import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_connection(retries = 5, delay = 3):
    attempt = 0
    while attempt < retries:
        try:
            connection = pymysql.connect(
                host="172.188.121.85",
                user="root",
                password="1qaz0plm",
                database="umls",
                port=3306,
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10
            )
            print("Connected to the database.")
            return connection
        except pymysql.MySQLError as e:
            attempt += 1
            logging.error(f"Connection attempt {attempt} failed: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error("All connection attempts failed. Please check the MySQL server and network settings.")
                raise e
            
def look_up_cui(term):
    """ Retrieve the Concept Unique Identifier (CUI) for a given term from the MRCONSO table in the UMLS database."""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT DISTINCT 
                    CUI
                FROM 
                    MRCONSO
                WHERE 
                    (STR = %s OR STR LIKE %s)
                    AND TTY = 'PT'
                    AND LAT = 'ENG';
            """
            cursor.execute(sql, (term, f"{term}%"))
            result = cursor.fetchall()
            if result:
                return result
            if not result:
                return None
    finally:
        connection.close()
            
def get_term(cui):
    """ Retrieve the preferred term for a given CUI from the MRCONSO table in the UMLS database."""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT DISTINCT 
                    STR
                FROM 
                    MRCONSO
                WHERE 
                    CUI = %s
                    AND TTY = 'PT'
                    AND LAT = 'ENG';
            """
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                return result
            if not result:
                return [{"STR": "Unknown Term"}]
    finally:
        connection.close()
        
def get_synonyms(cui):
    """ Retrieve synonyms for a given CUI from the MRCONSO table in the UMLS database. """
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT STR FROM MRCONSO WHERE CUI = %s AND TS = 'P' AND STT = 'PF'"
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                return result
            if not result:
                return None
    finally:
        connection.close()
            
def get_definition(cui):
    """ Retrieve the definition for a given CUI from the MRDEF table in the UMLS database."""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = "SELECT DEF FROM MRDEF WHERE CUI = %s"
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                return result
            if not result:
                return None
    finally:
        connection.close()
        
def get_semantic_type(cui):
    """ Retrieve the semantic type for a given CUI from the UMLS database. """
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT DISTINCT 
                    TUI, STY
                FROM 
                    MRSTY
                WHERE 
                    CUI = %s;
            """
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                return result
            if not result:
                return None
    finally:
        connection.close()
        
def get_relations(cui):
    """
    Retrieve all relationships for a given CUI from the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for which to retrieve relationships.

    Returns:
        list[dict]: A list of dictionaries containing all relationships, 
                    or None if no relationships are found.
    """
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # SQL query to find all relationships
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS SourceCUI,  
                    M1.STR AS SourceTerm,  
                    R.REL AS Relationship, 
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS TargetCUI, 
                    M2.STR AS TargetTerm, 
                    R.SAB AS Source
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    (R.CUI1 = %s OR R.CUI2 = %s) -- Match the input CUI as Source or Target
                    AND M1.TTY = 'PT'
                    AND M1.TS = 'P'
                    AND M2.TTY = 'PT'
                    AND M2.TS = 'P'
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG';
            """
            # Execute the query with the provided CUI
            cursor.execute(sql, (cui, cui))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "SourceCUI": row["SourceCUI"],
                        "SourceTerm": row["SourceTerm"],
                        "Relationship": row["Relationship"],
                        "RelationshipType": row["RelationshipType"],
                        "TargetCUI": row["TargetCUI"],
                        "TargetTerm": row["TargetTerm"],
                        "Source": row["Source"],
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No relationships found for CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching all relationships: {e}")
        return None
    finally:
        connection.close()
        
def get_specific_relation(cui, relationship_type):
    """
    Retrieve specific relationships for a given CUI from the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for which to retrieve relationships.
        relationship_type (str): The type of relationship to retrieve (e.g., 'children', 'parents', 'descendents').

    Returns:
        list[dict]: A list of dictionaries containing the specific relationships, 
                    or None if no relationships are found.
    """
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            # SQL query to find specific relationships
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS SourceCUI,  
                    M1.STR AS SourceTerm,  
                    R.REL AS Relationship, 
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS TargetCUI, 
                    M2.STR AS TargetTerm, 
                    R.SAB AS Source
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    (R.CUI1 = %s OR R.CUI2 = %s) -- Match the input CUI as Source or Target
                    AND R.RELA = %s
                    AND M1.TTY = 'PT'
                    AND M1.TS = 'P'
                    AND M2.TTY = 'PT'
                    AND M2.TS = 'P'
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG';
            """
            # Execute the query with the provided CUI and relationship type
            cursor.execute(sql, (cui, cui, relationship_type))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "SourceCUI": row["SourceCUI"],
                        "SourceTerm": row["SourceTerm"],
                        "Relationship": row["Relationship"],
                        "RelationshipType": row["RelationshipType"],
                        "TargetCUI": row["TargetCUI"],
                        "TargetTerm": row["TargetTerm"],
                        "Source": row["Source"],
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No '{relationship_type}' relationships found for CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching specific relationships: {e}")
        return None
    finally:
        connection.close()

def get_ro_relations(cui):
    """
    Retrieve 'RO' (Related To) relationships for a given CUI from the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for which to retrieve relationships.

    Returns:
        list[dict]: A list of dictionaries containing 'RO' relationships, or None if no relationships are found.
    """
    connection = get_connection()  # Assumes `get_connection` returns a valid database connection.
    try:
        with connection.cursor() as cursor:
            # SQL query to fetch RO relationships
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS SourceCUI, 
                    M1.STR AS SourceTerm,
                    R.CUI2 AS TargetCUI, 
                    M2.STR AS TargetTerm,
                    R.REL AS Relationship, 
                    R.RELA AS RelationshipType, 
                    R.SAB AS Source
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI
                WHERE 
                    R.CUI1 = %s
                    AND R.REL = 'RO'
                    AND R.SAB != 'NCI'
                    AND M1.LAT = 'ENG'
                    AND M1.TTY = 'PT' 
                    AND M2.LAT = 'ENG'
                    AND M2.TTY = 'PT';
            """
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                return [
                    {
                        "SourceCUI": row["SourceCUI"],
                        "SourceTerm": row["SourceTerm"],
                        "TargetCUI": row["TargetCUI"],
                        "TargetTerm": row["TargetTerm"],
                        "Relationship": row["Relationship"],
                        "RelationshipType": row["RelationshipType"],
                        "Source": row["Source"]
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No RO relationships found for CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching RO relationships: {e}")
        return None
    finally:
        connection.close()
        
            
def get_parent_from_snomedct(cui):
    """
    Retrieve 'isa' parent relationships for a given CUI from the SNOMEDCT_US database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for which to retrieve parent relationships.

    Returns:
        list[dict]: A list of dictionaries containing the parent relationships, 
                    or None if no parents are found.
    """
    connection = get_connection()  # Assumes `get_connection` is a valid function returning a database connection.
    try:
        with connection.cursor() as cursor:
            # SQL query to find 'isa' parent relationships
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS ChildID,  
                    M1.STR AS ChildTerm,  
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS ParentID, 
                    M2.STR AS ParentTerm
                FROM 
                    MRREL R
                JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    R.CUI1 = %s
                    AND R.RELA = 'inverse_isa'
                    AND R.REL = 'PAR'
                    AND R.SAB = 'SNOMEDCT_US'
                    AND M1.SAB = 'SNOMEDCT_US'
                    AND M1.TTY = 'PT'   
                    AND M2.SAB = 'SNOMEDCT_US'
                    AND M2.TTY = 'PT' 
                    AND M2.TS = 'P'   
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG';
            """
            # Execute the query with the provided CUI
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "ChildID": row["ChildID"],
                        "ChildTerm": row["ChildTerm"],
                        "RelationshipType": 'isa',
                        "ParentID": row["ParentID"],
                        "ParentTerm": row["ParentTerm"]
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No parents found for CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching isa parents: {e}")
        return None
    finally:
        connection.close()
        
def get_children_from_snomedct(cui):
    """
    Retrieve 'inverse_isa' child relationships for a given CUI from the SNOMEDCT_US database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for which to retrieve child relationships.

    Returns:
        list[dict]: A list of dictionaries containing child and parent relationships, 
                    or None if no children are found.
    """
    connection = get_connection()  # Assumes `get_connection` is a valid function returning a database connection.
    try:
        with connection.cursor() as cursor:
            # SQL query to find 'inverse_isa' child relationships
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS ChildID,  
                    M1.STR AS ChildTerm,  
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS ParentID, 
                    M2.STR AS ParentTerm
                FROM 
                    MRREL R
                JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    R.CUI2 = %s  -- Replace the placeholder with the provided CUI
                    AND R.RELA = 'inverse_isa'
                    AND R.REL = 'PAR'
                    AND R.SAB = 'SNOMEDCT_US'      
                    AND M1.SAB = 'SNOMEDCT_US'   
                    AND M1.TTY = 'PT'
                    AND M2.SAB = 'SNOMEDCT_US'    
                    AND M2.TTY = 'PT';
            """
            # Execute the query with the provided CUI
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "ParentID": row["ParentID"],
                        "ParentTerm": row["ParentTerm"],
                        "RelationshipType": 'isa',
                        "ChildID": row["ChildID"],
                        "ChildTerm": row["ChildTerm"],
                    }
                    for row in result
                ]
            else:
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching inverse_isa children: {e}")
        return None
    finally:
        connection.close()
        
def get_treatments(cui):
    """
    Retrieve treatments for a given disease CUI from the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for the disease.

    Returns:
        list[dict]: A list of dictionaries containing treatments for the disease, 
                    or None if no treatments are found.
    """
    connection = get_connection()  # Assumes `get_connection` is a valid function returning a database connection.
    try:
        with connection.cursor() as cursor:
            # SQL query to find treatments
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS DiseaseCUI,  
                    M1.STR AS DiseaseTerm,  
                    R.REL AS Relationship, 
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS TreatmentCUI, 
                    M2.STR AS TreatmentTerm
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    R.CUI1 = %s
                    AND R.RELA IN ('may_treat') -- Treatment relationships
                    AND M1.TTY = 'PT'
                    AND M2.TTY = 'PT'
                    AND M2.TS = 'P'
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG'
                GROUP BY 
                    R.CUI1, R.CUI2;
            """
            # Execute the query with the provided CUI
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "DiseaseCUI": row["DiseaseCUI"],
                        "DiseaseTerm": row["DiseaseTerm"],
                        "Relationship": row["Relationship"],
                        "RelationshipType": row["RelationshipType"],
                        "TreatmentCUI": row["TreatmentCUI"],
                        "TreatmentTerm": row["TreatmentTerm"]
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No treatments found for Disease CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching treatments: {e}")
        return None
    finally:
        connection.close()
        
def has_manifestation(cui):
    """
    Check if a given CUI has a manifestation relationship in the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) to check for manifestation relationships.

    Returns:
        bool: True if the CUI has a manifestation relationship, False otherwise.
    """
    connection = get_connection()  # Assumes `get_connection` is a valid function returning a database connection.
    try:
         with connection.cursor() as cursor:
            # SQL query to find treatments
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS DiseaseCUI,  
                    M1.STR AS DiseaseTerm,  
                    R.REL AS Relationship, 
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS TreatmentCUI, 
                    M2.STR AS TreatmentTerm
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    R.CUI1 = %s
                    AND R.RELA IN ('has_manifestation', 'manifestation_of') -- Treatment relationships
                    AND M1.SAB = 'SNOMEDCT_US'
                    AND M1.TS = 'P'
                    AND M1.TTY = 'PT'
                    AND M2.SAB = 'SNOMEDCT_US'
                    AND M2.TS = 'P'
                    AND M2.TTY = 'PT'
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG';
            """
            # Execute the query with the provided CUI
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "DiseaseCUI": row["DiseaseCUI"],
                        "DiseaseTerm": row["DiseaseTerm"],
                        "Relationship": row["Relationship"],
                        "RelationshipType": row["RelationshipType"],
                        "TreatmentCUI": row["TreatmentCUI"],
                        "TreatmentTerm": row["TreatmentTerm"]
                    }
                    for row in result
                ]
            else:
                return None
    except Exception as e:
        logging.error(f"An error occurred while checking for manifestation relationships: {e}")
        return None
    finally:
        connection.close()
        
def has_associated_finding(cui):
    """
    Retrieve 'has_associated_finding' relationships for a given CUI from the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) to query.

    Returns:
        list[dict]: A list of dictionaries containing associated findings, 
                    or None if no associated findings are found.
    """
    connection = get_connection()  # Assumes `get_connection` is a valid function returning a database connection.
    try:
        with connection.cursor() as cursor:
            # SQL query to find associated findings
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS SourceCUI,  
                    M1.STR AS SourceTerm,  
                    R.REL AS Relationship, 
                    R.RELA AS RelationshipType, 
                    R.CUI2 AS TargetCUI, 
                    M2.STR AS TargetTerm
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI  
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI  
                WHERE 
                    R.CUI1 = %s
                    AND R.RELA IN ('has_associated_finding', 'associated_finding_of', 'see_from', 'see', 'interprets', 'is_interpreted_by')
                    AND M1.SAB = 'SNOMEDCT_US'
                    AND M1.TS = 'P'
                    AND M1.TTY = 'PT'
                    AND M2.SAB = 'SNOMEDCT_US'
                    AND M2.TS = 'P'
                    AND M2.TTY = 'PT'
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG';
            """
            # Execute the query with the provided CUI
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                # Return in a clear and structured format
                return [
                    {
                        "SourceCUI": row["SourceCUI"],
                        "SourceTerm": row["SourceTerm"],
                        "Relationship": row["Relationship"],
                        "RelationshipType": row["RelationshipType"],
                        "TargetCUI": row["TargetCUI"],
                        "TargetTerm": row["TargetTerm"]
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No associated findings found for CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching associated findings: {e}")
        return None
    finally:
        connection.close()
        
        
def get_tradename(cui):
    """
    Retrieve tradenames for a given substance CUI from the UMLS database.

    Args:
        cui (str): The Concept Unique Identifier (CUI) for the substance.

    Returns:
        list[dict]: A list of dictionaries containing tradenames, or None if no tradenames are found.
    """
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            sql = """
                SELECT DISTINCT 
                    R.CUI1 AS SubstanceCUI, 
                    M1.STR AS SubstanceTerm, 
                    R.REL AS Relationship, 
                    R.RELA AS SpecificRelationship, 
                    R.CUI2 AS TradenameCUI, 
                    M2.STR AS Tradename
                FROM 
                    MRREL R
                LEFT JOIN 
                    MRCONSO M1 ON R.CUI1 = M1.CUI
                LEFT JOIN 
                    MRCONSO M2 ON R.CUI2 = M2.CUI
                WHERE 
                    R.CUI1 = %s
                    AND R.RELA = 'tradename_of'
                    AND M1.LAT = 'ENG'
                    AND M2.LAT = 'ENG'
                GROUP BY 
                    R.CUI1, R.CUI2;
            """
            cursor.execute(sql, (cui,))
            result = cursor.fetchall()
            if result:
                return [
                    {
                        "SubstanceCUI": row["SubstanceCUI"],
                        "SubstanceTerm": row["SubstanceTerm"],
                        "Relationship": row["Relationship"],
                        "SpecificRelationship": row["SpecificRelationship"],
                        "TradenameCUI": row["TradenameCUI"],
                        "Tradename": row["Tradename"]
                    }
                    for row in result
                ]
            else:
                logging.debug(f"No tradenames found for Substance CUI: {cui}")
                return None
    except Exception as e:
        logging.error(f"An error occurred while fetching tradenames: {e}")
        return None
    finally:
        connection.close()
        
        
# test
if __name__ == "__main__":
    # Example usage
    cui = "C0000737"  # Example CUI
    print("CUI:", cui)
    
    # Look up CUI
    print("Look up CUI:", look_up_cui("Aspirin"))
    
    """
    
    # Get term
    print("Get term:", get_term(cui))
    
    # Get synonyms
    print("Get synonyms:", get_synonyms(cui))
    
    # Get definition
    print("Get definition:", get_definition(cui))
    
    # Get semantic type
    print("Get semantic type:", get_semantic_type(cui))
    
    # Get relations
    print("Get relations:", get_relations(cui))
    
    # Get specific relation
    print("Get specific relation (children):", get_specific_relation(cui, 'children'))
    
    # Get RO relations
    print("Get RO relations:", get_ro_relations(cui))
    
    # Get parent from SNOMEDCT
    print("Get parent from SNOMEDCT:", get_parent_from_snomedct(cui))
    
    # Get children from SNOMEDCT
    print("Get children from SNOMEDCT:", get_children_from_snomedct(cui))
    
    # Get treatments
    print("Get treatments:", get_treatments(cui))
    
    # Check for manifestation
    print("Check for manifestation:", has_manifestation(cui))
    
    # Check for associated finding
    print("Check for associated finding:", has_associated_finding(cui))
    
    """