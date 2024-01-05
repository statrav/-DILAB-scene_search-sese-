# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 10:54:48 2023

@author: DILab
"""

import neointerface
from neo4j import GraphDatabase
from py2neo import Graph
import pandas as pd
import time
import mysql.connector
import pandas as pd
from gensim.models import KeyedVectors
from IPython.display import YouTubeVideo
from tabulate import tabulate
import scipy.io
import csv
import pymysql

pd.set_option('display.max_colwidth', None)

###################
## 1. connection ##
###################

class SESE:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, mariadb_user, mariadb_password, mariadb_host, mariadb_database):

        self.neo = neointerface.NeoInterface(host=neo4j_uri , credentials=(neo4j_user, neo4j_password))        
        self.graph = Graph(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        
        sql = mysql.connector.connect(user=mariadb_user, password=mariadb_password, host=mariadb_host)
        sqlcnx = sql.cursor()
        sqlcnx.execute("CREATE DATABASE IF NOT EXISTS {}".format(mariadb_database))    
        
        self.cnx = mysql.connector.connect(user=mariadb_user, password=mariadb_password, host=mariadb_host, database=mariadb_database)
        
        q = "CALL dbms.components() YIELD name, versions"
        session = self.driver.session()
        result = session.run(q)
        
        print("-- Successfully connected! --")
        print("connection neo4j, mariadb user name : {}, {}".format(neo4j_user, mariadb_user))
        print("current neo4j version ", result.data()[0]['versions'][0])
        
                
    def close(self):
        self.neo.close()

    ###############
    ## 2. create ##
    ###############
    def add(self, file):
        try:
            q = "CREATE INDEX ON :object(video_id);"
            session = self.driver.session()                    
            result = session.run(q)
            # print("index ok")
            
        except Exception as e:
            pass

        old_q = "MATCH(n) RETURN count(n)"
        session = self.driver.session()                    
        old_count = session.run(old_q)
        
        if old_count:
            old_count = old_count.data()[0]['count(n)']
        else:
            old_count = 0

        q = f"""
            LOAD CSV WITH HEADERS FROM 'file:///{file}' AS row
            WITH row
            WHERE NOT (row.subject IS NULL OR row.video_id IS NULL)
            MERGE (o1:object {{video_id: row.video_id, object: row.subject}})
            ON CREATE SET o1.video_id = row.video_id, o1.object = row.subject

            WITH row
            WHERE NOT (row.object IS NULL OR row.video_id IS NULL)
            MERGE (o2:object {{video_id: row.video_id, object: row.object}})
            ON CREATE SET o2.video_id = row.video_id, o2.object = row.object
            """
        start_time_node = time.time()    
        session = self.driver.session()                    
        node_res = session.run(q)
        end_time_node = time.time()
        time_node = end_time_node - start_time_node
        # print("node ok")

        q = f"""
            CALL apoc.load.csv('{file}') YIELD lineNo, map AS row
            WITH row WHERE NOT (row.subject IS NULL OR row.predicate IS NULL OR row.object IS NULL)
            CALL {{
            WITH row
            MERGE (s:object {{video_id: row.video_id, object: row.subject}})
            MERGE (o:object {{video_id: row.video_id, object: row.object}})
            WITH s, o, row.predicate AS edgeLabel, row
            WHERE NOT (row.predicate IS NULL OR trim(row.predicate) = '')
            CALL {{
                WITH s, o, edgeLabel, row
                CALL apoc.create.relationship(s, edgeLabel, {{}}, o) YIELD rel
                SET rel += {{
                video_id: row.video_id,
                video_path: row.video_path,
                captions: row.captions,
                begin_frame: row.begin_frame,
                end_frame: row.end_frame,
                subject: row.subject,
                predicate: row.predicate,
                object: row.object
                }}
                RETURN COUNT(rel) AS processedRows, type(rel) AS relType
            }}
            RETURN SUM(processedRows) AS totalProcessedRows, collect(DISTINCT relType) AS uniqueRelTypes
            }}
            WITH sum(totalProcessedRows) AS sum_totalProcessedRows, uniqueRelTypes
            RETURN sum(sum_totalProcessedRows) as n_spo, count(uniqueRelTypes) as n_type
            """
        
        start_time_edge= time.time()
        session = self.driver.session()                    
        rel_res = session.run(q)
        end_time_edge = time.time()
        # print("rel ok")
        time_edge = end_time_edge - start_time_edge

        # extract n_spo and n_type
        result = rel_res.data()[0] 
        n_spo = result['n_spo']
        n_type = result['n_type']

        # finally,
        new_q = "MATCH(n) RETURN count(n)"
        session = self.driver.session()                    
        new_count = session.run(new_q)
        load_count = new_count.data()[0]['count(n)'] - old_count

        print(f"Load the {load_count} objects successfully.")
        print(f"Load the {n_type} relationships and {n_spo} spos successfully.")
        print( )
        print("total time elapsed: ", time_node + time_edge)
        print( )
        print("--please wait for generating page rank--")
        
        ########################
        ## generate page rank ##
        ########################
        # generate comtomizing weight
        query = """
            MATCH (a)-[]->(b)<-[]-(c)
            WHERE id(a) > id(c)
            WITH a, b, c, count(*) as weight
            MERGE (a)-[r:Inter]->(c)
            ON CREATE SET r.w = weight
            """
    
        session = self.driver.session()                    
        result = session.run(query)
    
        # generate temp graph
        query = "CALL gds.graph.create('Graph_Inter', 'object', 'Inter', {relationshipProperties: 'w'})"
        session = self.driver.session()                    
        session.run(query)
        # print("make inter")/
        # generate pageRank
        query = """
            CALL gds.pageRank.write('Graph_Inter', 
            {maxIterations: 20, dampingFactor: 0.85, relationshipWeightProperty: 'w', writeProperty: 'pagerank'})
            YIELD nodePropertiesWritten, ranIterations
            """
        session = self.driver.session()                    
        session.run(query)
        # print("make pagerank")
        
        # remove temp graph
        query = "CALL gds.graph.drop('Graph_Inter');"
        session = self.driver.session()                    
        session.run(query)
    
        # remove temp relation
        query = "MATCH p=()-[r:Inter]->() detach delete r;"
        session = self.driver.session()                    
        session.run(query)
        
    def add_table(self, mariadb_database, csv_file):
        cursor = self.cnx.cursor()
        
        cursor.execute('DROP TABLE IF EXISTS activitynet')

        cursor.execute('''
        CREATE TABLE `activitynet` (
        	`video_id` VARCHAR(20) NOT NULL COLLATE 'utf8mb4_general_ci',
        	`video_path` VARCHAR(50) NOT NULL COLLATE 'utf8mb4_general_ci',
        	`begin_frame` DECIMAL(20,6) NULL,
        	`end_frame` DECIMAL(20,6) NOT NULL,
        	`captions` MEDIUMTEXT NOT NULL COLLATE 'utf8mb4_general_ci',
        	`subject` VARCHAR(20) NULL COLLATE 'utf8mb4_general_ci',
        	`predicate` VARCHAR(20) NULL COLLATE 'utf8mb4_general_ci',
        	`object` VARCHAR(20) NULL COLLATE 'utf8mb4_general_ci'
        )

        COLLATE='utf8mb4_general_ci'
        ENGINE=InnoDB
        ''')

        cursor.execute('''
        LOAD DATA LOCAL INFILE '{}' 
        REPLACE INTO TABLE `{}`.`activitynet` 
        CHARACTER SET euckr 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        ESCAPED BY '"' 
        LINES TERMINATED BY '\r\n' 
        IGNORE 1 LINES 
        (`video_id`, `video_path`, @ColVar2, @ColVar3, `captions`, `subject`, `predicate`, `object`) 
        SET `begin_frame` = REPLACE(REPLACE(@ColVar2, ',', ''), '.', '.'), 
        `end_frame` = REPLACE(REPLACE(@ColVar3, ',', ''), '.', '.')
        '''.format(csv_file, mariadb_database))
        
        print("MariaDB Load Successfully!")
        
    def add_db(self, mariadb_database, csv_file):
            
        self.add_table(mariadb_database, csv_file)
            
        df = pd.read_csv(csv_file, encoding='cp949')
            
        df_sub = df[['subject', 'video_id']]
        df_sub.columns = ['object', 'video_id']
    
        df_pre = df[['predicate', 'video_id']]
        df_pre.columns = ['object', 'video_id']
    
        df_obj = df[['object', 'video_id']]
        df_obj.columns = ['object', 'video_id']
    
        obj_df = pd.concat([df_sub, df_pre, df_obj], axis=0)
            
        self.add_object(obj_df)
        self.add_spo(df) 
       
    ################
    ##  3. search ##
    ################
    
    ## rdb using part    
        
    def get_keyword(self, w2v_file = './pre-trained_model/activity_w2v'):
        quote = []
        quote = list(map(str, input("Enter the keyword you want to search for. Separate multiple entries with a comma(,). : ").split(',')))
        query = self.make_quotes(quote)
        start = time.time()
        cursor = self.cnx.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns = ['video', 'starts', 'ends', 'captions'])
        df["count"] = self.count(quote, df)
        df = df.sort_values(by=["count"], ascending=False)
        df = df.reset_index(drop=True)
        print("Search Keywords : {}".format(quote))
        
        try:
            #token 확장
            if len(df) == 0:
                keywords = self.w2v(quote, w2v_file)
                query2 = self.make_quotes_w2v(keywords)
                cursor.execute(query2)
                result2 = cursor.fetchall()
                df = pd.DataFrame(result2, columns = ['video', 'starts', 'ends', 'captions'])
                df["count"] = self.count(quote, df)
                df = df.sort_values(by=["count"], ascending=False)
                df = df.reset_index(drop=True)
                print("There are no scenes searched by the keyword you entered.")
                print("We will proceed with the search including similar words.")
                print("Search & Extension Keywords : {}".format(keywords))
        except:
           df = ""
           print("We can't found the appropriate keyword for your search.")
                
        end = time.time()        
        print(f"The time required : {end - start:.5f} sec")
        print("Number of result values : ", len(df))
        print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

        #video
        if len(df) !=0:            
            video = self.embed_video(df["video"].loc[0])   
        else:
            video = "No appropriate scene found."

        return video

    def make_quotes(self, ls):
        quote = "SELECT video_path, begin_frame, end_frame, captions FROM activitynet WHERE captions LIKE '%{}%'".format(ls[0])
        if len(ls) > 1:
            for idx, word in enumerate(ls):
                if idx > 0:
                    special_token = " OR captions LIKE '%{}%'".format(ls[idx])
                    quote += special_token
        return quote

    def make_quotes_w2v(self, ls):
        quote = "SELECT video_path, begin_frame, end_frame, captions FROM activitynet WHERE (captions LIKE '%{}%'".format(ls[0])
        if len(ls)>1:
            for idx, word in enumerate(ls):
                if idx > 0:
                    if idx %2 != 0:
                        special_token = " OR captions LIKE '%{}%')".format(ls[idx])
                    if idx %2 == 0 :
                        special_token = " AND (captions LIKE '%{}%'".format(ls[idx])
                    quote += special_token
        return quote
        
    def w2v(self, query, w2v_file):
        n = 1
        result = []
        model = KeyedVectors.load(w2v_file)
            
        for word in query:
            similar = []
            result.append(word)
            similar.append(model.wv.most_similar(word))
            for j in similar:
                for num in range(n):
                    result.append(j[num][0])
        return list(set(result))
        
    def embed_video(self, url):
        embed_url = url
        embed_id = embed_url[32:]
        video = YouTubeVideo(embed_id, width=400)
        return video         

    def count(self, ls, df):
        num = 0
        for word in ls:
            if word != "":
                num += df["captions"].str.count(word)
            else:
                num += 0
        return num

    ## graphdb using part
    def get_description(self):
        
        ## first: count of nodes and relationships in DB ##
        query = f"""
            MATCH (n)
            RETURN count(n) as node_count
            """
        session = self.driver.session()
        result1 = session.run(query)
        result1 = result1.data()
    
        query = f"""
            MATCH ()-->() RETURN count(*) as relationship_count; 
            """
        session = self.driver.session()
        result2 = session.run(query)
        result2 = result2.data()
        out1 = [dict(d1, **d2) for d1, d2 in zip(result1, result2)]
        
        ## second: information in DB ##
        ## What kind of nodes exist
        ## Sample some nodes, reporting on property and relationship counts per node.
        query = f"""
            MATCH (n) WHERE rand() <= 0.1
            RETURN
            DISTINCT labels(n) as node_label,
            count(*) AS SampleSize,
            avg(size(keys(n))) as Avg_PropertyCount,
            min(size(keys(n))) as Min_PropertyCount,
            max(size(keys(n))) as Max_PropertyCount,
            avg(size( (n)-[]-() ) ) as Avg_RelationshipCount,
            min(size( (n)-[]-() ) ) as Min_RelationshipCount,
            max(size( (n)-[]-() ) ) as Max_RelationshipCount
            """
        session = self.driver.session()
        result = session.run(query)
        out2 = result.data()
        
        # result
        print('----node and relation count----')
        print(tabulate(out1, headers='keys', tablefmt='psql', showindex=False))
        print('\n')
        print('----Information for property and relationship----')
        print(tabulate(out2, headers='keys', tablefmt='psql', showindex=False))
        return out1, out2
                
    def get_object_list(self):
        
        query = f"""
            MATCH (n:object)
            RETURN distinct n.object as object;
            """
        session = self.driver.session()
        result = session.run(query)
        df_result = pd.DataFrame(result)
        result = list(df_result[0])
        result = list(set(result))
        return result

    def get_object(self, object = False):

        q_match = f"MATCH (n) "
        
        q_with = f"WITH *"
        
        if object:
            obj = object.split(',')
            for i, ob in enumerate(obj):
                ob = ob.replace(' ', '')
                if i == 0:
                    q_obj = f"n.object = '{ob}'"
                else:
                    q_obj = q_obj + f" or n.object = '{ob}' "
            q_obj = "(" + q_obj +")"

        else:
            q_obj = ''

        if object:
            q_where = f"WHERE "+ q_obj + '\n'
        else:
            q_where = '\n'

        q_return = f"RETURN n.object as object, n.video_id as video_id;"

        query = q_match + '\n'+ q_with + '\n'+ q_where + '\n' + q_return
        start_time = time.time()
        
        session = self.driver.session()                    
        result = session.run(query)
        
        end_time = time.time()
        out = result.data()
        df_result = pd.DataFrame(out)   

        print("total time elapsed: ", end_time-start_time)
        return df_result

    def get_predicate_list(self):

        query = f"""
            CALL db.relationshipTypes()
            """
        session = self.driver.session()
        result = session.run(query)
        df_result = pd.DataFrame(result)
        results = list(df_result[0])        
        return results

    def get_spo(self, video_id = False, subject = False, sp_link = False, object = False, po_link = False, so_link = False, predicate = False, w2v_file = './pre-trained_model/activity_w2v'):
        
        ######################
        ## get spo function ##
        ######################
        print("video_id:")
        video_id_list = input()
        print(video_id_list)
    
        print("subject:")
        subject = input()
        print(subject)
        
        if subject == '':
            subject = False
                        
        print("object:")
        object = input()
        print(object)

        if object == '':
            object = False

        print("predicate:")
        predicate = input()
        print(predicate)

        if predicate == '':
            predicate = False
        
        if subject and object:
            print("How to link subjects and and objects?")
            print("If you use AND, the spo satisfying both subject and object is searched. If you use OR, the spo satisfying either the subject or the object is searched.")
            so_link = input()
            print(so_link)

        if subject and predicate:
            print("How to link subjects and and predicates?")
            print("If you use AND, the spo satisfying both subject and predicates is searched. If you use OR, the spo satisfying either the subject or the predicates is searched.")
            sp_link = input()
            print(sp_link)
            
        if predicate and object:
            print("How to link predicates and and objects?")
            print("If you use AND, the spo satisfying both predicates and objects is searched. If you use OR, the spo satisfying either the predicates or the objects is searched.")
            po_link = input()
            print(po_link)
            
        if video_id_list:
            video_id_list = video_id_list.split(', ')
            video_id = []
            for i, r in enumerate(video_id_list):
                video_id.append(r)
        else:
            video_id = False
        
        if subject and predicate:
            if not sp_link:
                sp_link = ' and '
            else:
                sp_link = sp_link
        elif not predicate or not object:
            sp_link = ''
        elif not subject:
            sp_link = ''

        if predicate and object:
            if not po_link:
                po_link = ' and '
            else:
                po_link = po_link
        elif not object or not subject:
            po_link = ''

        if subject and object:
            if not so_link:
                so_link = ' and '
            else:
                so_link = so_link
        elif not subject or not predicate:
            so_link = ''

        match = f"MATCH (s:object)-[r]->(o:object) "
        
        if not subject:
            s_where = ' '
        else:
            subj = subject.split(', ')
            for i, sub in enumerate(subj):
                # sub = sub.replace(' ', '')
                if i == 0:
                    s_where = f" (startNode(r).object = '{sub}' "
                else:
                    s_where = s_where + f" or startNode(r).object = '{sub}' "
            s_where = s_where + ") "
            # s_where = f" startNode(r).object IN {subject} "
        
        if not object:
            o_where = ' '
        else:
            obj = object.split(', ')
            for i, ob in enumerate(obj):
                # sub = sub.replace(' ', '')
                if i == 0:
                    o_where = f" (endNode(r).object = '{ob}' "
                else:
                    o_where = o_where + f" or endNode(r).object = '{ob}' "
            o_where = o_where + ") "            
            # o_where = f" endNode(r).object IN {object} "
            
        if not predicate:
            p_where = ' '
        else:
            pred = predicate.split(', ')
            for i, prd in enumerate(pred):
                # sub = sub.replace(' ', '')
                if i == 0:
                    p_where = f" (type(r) = '{prd}' "
                else:
                    p_where = p_where + f" or type(r) = '{prd}' "
            p_where = p_where + ") "
            # p_where = f" type(r) IN {predicate} "
        
        if video_id:
            w_video = ""
            for ii, vid in enumerate(video_id):
                if ii == 0:
                    w_video = w_video + f"r.video_id ='{vid}'"
                else:
                    w_video = w_video + f" or r.video_id ='{vid}'"
            w_video = "(" + w_video + ")"

        if subject and object and not predicate:
            where = "WHERE (" + s_where + so_link + o_where + ")"
        elif so_link == 'and' and po_link == 'or':
            where = "WHERE (" + s_where + so_link + o_where + po_link + p_where + ")"
        else:
            where = "WHERE (" + s_where + sp_link + p_where + po_link + o_where + ")"

        if video_id:
            where = where + " and " + w_video
        else:
            where = where 

        if not subject and not object and not predicate:
            where = ' '
            if video_id_list:
                where = "where " + w_video 
        
        with_q = "WITH r.video_id AS video_id, r.video_path AS video_path, r.captions AS captions, properties(r) AS prop_r, type(r) AS predicate, startNode(r) AS startNode, endNode(r) AS endNode, [startNode(r).object, type(r), endNode(r).object] AS spo, [properties(r).begin_frame, properties(r).end_frame] AS frame"
        
        if subject:
            with_s = " COLLECT(DISTINCT startNode.object) as sub_cond "
        else:
            with_s = ''
            
        if object:
            with_o = "COLLECT(DISTINCT endNode.object) as ob_cond"
        else:
            with_o = ''
        
        if predicate:
            with_p = "COLLECT(DISTINCT predicate) as pred_cond "
        else:
            with_p = ''
        
        with_spo = ''
        if subject:
            if not predicate and not object:
                with_spo = ', ' + with_s
            if predicate and not object:
                with_spo = ', ' + with_s + ', ' + with_p
            if not predicate and object:
                with_spo = ', ' + with_s + ', ' + with_o
            if predicate and object:
                with_spo = ', ' + with_s + ', ' + with_p + ', ' + with_o
        elif not subject:
            if predicate and not object:
                with_spo = ', ' + with_p
            elif not predicate and object:
                with_spo = ', ' + with_o
            elif predicate and object:
                with_spo = ', ' + with_p + ', ' + with_o
        
        with_q = with_q + '\n' + "WITH video_id, video_path, captions, collect(DISTINCT spo) as spo, collect(frame) as frame" + with_spo
                
        return_q = "RETURN video_id, video_path, captions, spo, frame"
        
        query = match + '\n' + where + '\n' + with_q + '\n' + return_q

        session = self.driver.session()
        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        print("total time elapsed: ", end_time-start_time)
        
        out = result.data()
        out = pd.DataFrame(out)

        print(tabulate(out, headers='keys', tablefmt='psql', showindex=False))
        print(f"Total number of retrieves : {len(out)}")
        
        ###########
        ## video ##
        ###########
        if len(out) !=0:            
            video = self.embed_video(out["video_path"][0])
        else:
            video = "No appropriate scene found."
            
        ###############
        ## expansion ##
        ###############
        exp_start_time = time.time()   
        if len(out) == 0:
            subj_w2v = ''
            obj_w2v = ''
            pred_w2v = ''
            
            if subject == False:
                s_where = ' '
            else:
                subj = subject.split(', ')
                subj_w2v = self.w2v(subj, w2v_file)
                for i, sub in enumerate(subj_w2v):
                    if i == 0:
                        s_where = f" (startNode(r).object = '{sub}' "
                    else:
                        s_where = s_where + f" or startNode(r).object = '{sub}' "
                s_where = s_where + ") "
                
            if object == False:
                o_where = ' '
            else:
                obj = object.split(', ')
                obj_w2v = self.w2v(obj, w2v_file)
                for i, ob in enumerate(obj_w2v):
                    # sub = sub.replace(' ', '')
                    if i == 0:
                        o_where = f" (endNode(r).object = '{ob}' "
                    else:
                        o_where = o_where + f" or endNode(r).object = '{ob}' "
                o_where = o_where + ") "            
                # o_where = f" endNode(r).object IN {object} "
        
            if predicate == False:
                p_where = ' '
            else:
                pred = predicate.split(', ')
                pred_w2v = self.w2v(pred, w2v_file)
                for i, prd in enumerate(pred_w2v):
                    if i == 0:
                        p_where = f" (type(r) = '{prd}' "
                    else:
                        p_where = p_where + f" or type(r) = '{prd}' "
                p_where = p_where + ") "
        
            if subject and object and predicate == False:
                where = "WHERE (" + s_where + so_link + o_where + ")"
            elif so_link == 'and' and po_link == 'or':
                where = "WHERE (" + s_where + so_link + o_where + po_link + p_where + ")"
            else:
                where = "WHERE (" + s_where + sp_link + p_where + po_link + o_where + ")"
         
            if subject == False and object == False and predicate == False:
                where = ' '

            query = match + '\n' + where + '\n' + with_q + '\n' + return_q

            session = self.driver.session()      
            result = session.run(query)
            exp_end_time = time.time()
            out = result.data()
            print("total time elapsed for expansion: ", exp_end_time-exp_start_time)
            out = pd.DataFrame(out) 
            print("")
            print("There are no scenes searched by the keyword you entered.")
            print("We will proceed with the search including similar words.")
            if subj_w2v:
                print("Subject - Search & Extension Keywords : {}".format(subj_w2v))
            if obj_w2v:
                print("Object - Search & Extension Keywords : {}".format(obj_w2v))
            if pred_w2v:
                print("Predicate - Search & Extension Keywords : {}".format(pred_w2v))
            print("")
            print(tabulate(out, headers='keys', tablefmt='psql', showindex=False))
            
            print(f"Total number of retrieves for expansion : {len(out)}")
            
            # video
            if len(out) !=0:            
                video = self.embed_video(out["video_path"][0])   
            else:
                video = "No appropriate scene found."
        
        return video
        # return out


    def get_Digraph(self, type, objects, predicates, step = 2):

        if type == 'tree':
            def add_nodename(lst):
                return ["(n" + str(num) + ")" for num in lst]

            n_num = range(step+1)
            n_name = add_nodename(n_num)
            q_match1 = ", ".join(n_name)
            q_match1 = "MATCH " + q_match1
            
            a_step = step
            if a_step >= 1:
                q_match2 = f"(n0)-[r0]->(n1)"
            if a_step >= 2:
                a_step = a_step - 1
                for n in range(a_step):
                    q_match2 = q_match2 + f"-[r{n+1}]->(n{n+2})"
            q_match2 = "MATCH " + q_match2
        
        if type == 'center':
            q_match1 = f"(n0)"
            for n in range(step):
                q_match1 = q_match1 + f", (n{n+1}) "
            q_match1 = "MATCH " + q_match1
            a_step = step
            if a_step >= 1:
                q_match2 = f"(n0)-[r0]->(n1)"
            if a_step >= 2:
                a_step = a_step - 1
                for n in range(a_step):
                    q_match2 = q_match2 + f"<-[r{n+1}]-(n{n+2})"
            q_match2 = "MATCH " + q_match2
        
        def add_relename(lst):
            return ["properties(r" + str(num) + ") as RelationshipProperty" + str(num) for num in lst]

        n_num = range(step)
        n_name = add_relename(n_num)
        q_with = ", ".join(n_name)
        q_with = "WITH *, " + q_with

        obj_where = ''
        objs_split = ''
        if len(objects) == 0:
            obj_where = obj_where
        elif len(objects) >= 1:
            k = 0
            l = 0
            for objs in objects:
                k = k + 1
                if objs:
                    l = l + 1
                    if l >= 2:
                        obj_where = obj_where + " and "
                    objs_split = objs.split(',')
                    for i, obj in enumerate(objs_split):
                        obj = obj.replace(' ', '')
                        if i == 0:
                            obj_where = obj_where + "("
                            obj_where = obj_where + f"n{k-1}.object = '{obj}'"
                        else:
                            obj_where = obj_where + f" or n{k-1}.object = '{obj}'"
                    obj_where = obj_where + ")"


        pred_where = ''
        preds_split = ''
        if len(predicates) == 0:
            pred_where = pred_where
        elif len(predicates) >= 1:
            k = 0
            l = 0
            for preds in predicates:
                k = k + 1
                if preds:
                    l = l + 1
                    if l >= 2:
                        pred_where = pred_where + " and "
                    preds_split = preds.split(',')
                    for i, pred in enumerate(preds_split):
                        pred = pred.replace(' ', '')
                        if i == 0:
                            pred_where = pred_where + "("
                            pred_where = pred_where + f"type(r{k-1}) = '{pred}'"
                        else:
                            pred_where = pred_where + f" or type(r{k-1}) = '{pred}'"
                    pred_where = pred_where + ")"

        if len(objs_split) == 0 and len(objs_split) == 0:
            q_where = ''
        elif len(objs_split) >= 1 and len(preds_split) == 0:
            q_where = "WHERE " + obj_where 
        elif len(preds_split) >= 1 and len(objs_split) == 0:
            q_where = "WHERE " + pred_where
        elif len(preds_split) >= 1 and len(objs_split) >= 0:
            q_where = "WHERE " + obj_where + ' and ' + pred_where
         
        q_return = ''
        for n in range(step):
            # q_return = q_return + f"n{n}.object as object{n+1}, type(r{n}) as predicate{n+1}, RelationshipProperty{n}.begin_frame as begin_frame{n+1}, RelationshipProperty{n}.end_frame as end_frame{n+1}, "
            q_return = q_return + f"n{n}.object as object{n+1}, type(r{n}) as predicate{n+1}, "
        q_return = f"RETURN distinct n0.video_id as video_id, " + q_return + f"n{n+1}.object as object{n+2}"

        query = q_match1 + '\n'+ q_match2 + '\n'+ q_with + '\n'+ q_where + '\n' + q_return
        print(query)
        session = self.driver.session()         

        start_time = time.time()
        result = session.run(query)
        end_time = time.time()
        print("total time elapsed: ", end_time-start_time)
        
        out = result.data()
        df_result = pd.DataFrame(out)
        return df_result