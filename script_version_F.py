# -*- coding: utf-8 -*-
"""
Created on Mon Mar  2 10:20:39 2020

@author: MOKHTAR
"""

import schedule
import time
import random
import string
import pymysql.cursors
from pymongo import MongoClient
def job():
     print("Début")
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('adherentavpsfm')




     collection = db.adherentavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
            'id', 'typePIDaAvp'  ,
            'numeroPIDaAvp',  'referenceCaAvp',  
            'nbrenfant', 'dateDemandeAffiliationCaAvp',  
            'statutsEnrolementCaAvp',  
            'sourceEnrolementCaAvp', 
            'user_created_id', 
            'numeroCarteCaAvp', 
            'raisonBlocageCaAvp',  
            'dateAffiliationCaAvp', 
            'validationInputCaAvp' , 
            'validationFileCaAvp'  ,
            'validationInputResponsableCaAvp',  
            'dateDepotCaAvp' ,
            'bureau_souscription_avp_id' ,
            'suspensionImAvp',
            'dateGenerationPdfEnrolementCaAvp', 
            'code_ligne_avp' ,
            'commentaireCaAvp', 
            'dateDernierRelanceCaAvp', 
            'markToDelete' ,
            'markToQualified', 
            'qualified',
            'agent_commercial_avp_id',
            'agent_commercial_qualified_avp_id'
            )
     query = ( 'SELECT  *  FROM adherentavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("adherentavpsfm en cours ...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("adherentavpsfm fini !")                                               
           
     cursor.close()
     cnx.close()
     
     
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('adherentcomplementavpsfm')




     collection = db.adherentcomplementavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',                                  
           'adherent_avp_id',                     
           'nomJeuneFilleDaAvp',                  
           'dateNaissanceDaAvp',                  
           'lieuNaissanceDaAvp',                  
           'nationaliteDaAvp',                    
           'sexeDaAvp',                           
           'situationMatrimonialDaAvp',           
           'domaineActiviteDaAvp',                
           'secteurActiviteDaAvp',                
           'adresseProfessionnelAvp',             
           'dateDebutExerciceDaAvp',              
           'neVersDaAvp',                         
           'typeDateNaissance',                   
           'codePhoneDaAvp',                      
           'operateur_phone_im_avp_id',           
           'operateur_phone2im_avp_id',           
           'operateur_phone3im_avp_id',           
           'codePhone2DaAvp',                     
           'contactTelephonique3DaAvp',           
           'codePhone3DaAvp', 
            )
     query = ( 'SELECT  *  FROM adherentcomplementavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("adherentcomplementavpsfm en cours ...")                                               
                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("adherentcomplementavpsfm fini !")                                               
           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('adresseavpsfm')




     collection = db.adresseavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'paysDaAvp',
           'villeDaAvp',                   
           'regionDaAvp',                  
           'discritDaAvp',                 
           'communeDaAvp',                 
           'cercleDaAvp',                  
           'quartierDaAvp',                
           'rueDaAvp',                     
           'porteDaAvp',                   
           'immeubleDaAvp',                
           'bpDaAvp',                      
           'faxDaAvp',                     
           'adressEtrangerDaAvp',          
           'emailDaAvp',                  
            )
     query = ( 'SELECT  *  FROM adresseavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("adresseavpsfm en cours ...")                                               
                                                    
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("adresseavpsfm fini !")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('bureauavpsfm')




     collection = db.bureauavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adresse',              
           'ville',
           'code_postal',                   
           'ip_adresse_passrelle',                  
           'label',                 
            )
     query = ( 'SELECT  *  FROM bureauavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("bureauavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("bureauavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('champtypenome')




     collection = db.champtypenome

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'CODE_CHAMP',              
           'NOM_CHAMP',
           'CODE_STR',                   
            )
     query = ( 'SELECT  *  FROM champtypenome;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("champtypenome en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
        
         nbr+=1
        
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("champtypenome fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('classeavpsfm')




     collection = db.classeavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'nomClasseAvp',
           'revenuTrimestrielDaAvp',
           'revenuAnnuelCaAvp',              
           'montantCotisationCaAvp',
           'dateInsertionImAvp',  
           'pathRevenuAnnuelImAvp',              
           'conformiteRevenuAnnuelDaAvp',
           'pathEngagementHonneurImAvp',
           'conformiteEngagementHonneurDaAvp',              
           'revenuMensuelDaAvp',
           'currentDaAvp',
           'dateActivationImAvp',
            )
     query = ( 'SELECT  *  FROM classeavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("classeavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("classeavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('codenomenclature')




     collection = db.codenomenclature

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'CODE_NOME',              
           'LIB_NOME',
           'ENABLED',
           'CODE_STR',              
            )
     query = ( 'SELECT  *  FROM codenomenclature;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("codenomenclature en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("codenomenclature fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('comptabilisationavpsfm')




     collection = db.comptabilisationavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'classe_comptabilite_avp_id',              
           'adherent_avp_id',
           'montant_cotisation',
           'date_cotisation',
           'date_comptabilisation',              
           'trimestre',
           'solde',
           'compteDebit',
           'compteCredit',              
           'date_limit_cotisation',
           'decaissement_avp_id',
           'decaisse',              
            )
     query = ( 'SELECT  *  FROM comptabilisationavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("comptabilisationavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("comptabilisationavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('cotisationavpsfm')




     collection = db.cotisationavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'comptabilisation_avp_id',              
           'dateCotisationImAvp',
           'montantPayeImAvp',
           'paiement_avp_id',
           'adherent_avp_id',              
            )
     query = ( 'SELECT  *  FROM cotisationavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("cotisationavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("cotisationavpsfm fini!")           
     cursor.close()
     cnx.close() 
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('decaissementavpsfm')




     collection = db.decaissementavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'montantGlobalCaAvp',              
           'dateCreationDecaissementImAvp',
           'exporte',
           'descriptionImAvp',
           'dateReversementInpsImAvp',
           'statutsReversementCaAvp',          
           'dateModificationDecaissementImAvp',              
           'referenceJustificatifReversementInpsImAvp',
           'pathJustificatifReversementInpsImAvp',
           'responsable_initiation_avp_id',
           'bureau_initiation_avp_id',
           'responsable_validation_montant_avp_id',          
           'bureau_validation_montant_avp_id',              
           'responsable_finalisation_avp_id',
           'bureau_finalisation_avp_id',
           'responsable_validation_avp_id',
           'bureau_validation_avp_id',
           'code_ligne_avp',
           'rembousementDaAvp',
            )
     query = ( 'SELECT  *  FROM decaissementavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("decaissementavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("decaissementavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('deletebulkavpsfm')




     collection = db.deletebulkavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'delete_success',              
           'delete_fail',
           'dateCotisationImAvp',
            )
     query = ( 'SELECT  *  FROM deletebulkavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("deletebulkavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("deletebulkavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('docenrolementavpsfm')




     collection = db.docenrolementavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'libelleDaAvp',
           'pathImAvp',
           'dateInsertionImAvp',
           'conformiteDaAvp',
            )
     query = ( 'SELECT  *  FROM docenrolementavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("docenrolementavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("docenrolementavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('groupe')




     collection = db.groupe

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'name',              
           'roles',
           'nbrUsers',
            )
     query = ( 'SELECT  *  FROM groupe;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("groupe en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("groupe fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('horaire')




     collection = db.horaire

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'jour',              
           'heure_debut',
           'heure_fin',
            )
     query = ( 'SELECT  *  FROM horaire;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("horaire en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("horaire fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('operateuravpsfm')




     collection = db.operateuravpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'nomOperateurImAvp',              
           'prefixesImAvp',
           'created_at',
           'updated_at',
            )
     query = ( 'SELECT  *  FROM operateuravpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("operateuravpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                            
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("operateuravpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('paiementavpsfm')




     collection = db.paiementavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'agent_caisse_avp_id',              
           'bureau_avp_sfm_id',
           'adherent_avp_id',
           'montant_global',              
           'numTransImAvp',
           'nomImAvp',  
           'prenomImAvp',              
           'tiersImAvp',
           'dateCotisationImAvp',
           'fraisImAvp',              
           'sourceImAvp',
           'machineImAvp',
           'ipMachineImAvp',
           'txnidImAvp',
           'numTelImAvp',
           'exporte',
           'code_ligne_avp',
           'periodeImAvp',
            )
     query = ( 'SELECT  *  FROM paiementavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("paiementavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("paiementavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('perception_prestation')




     collection = db.perception_prestation

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'residentDaAvp',
           'regionDaAvp',
           'districtDaAvp',              
           'bureauDaAvp',
           'paysDaAvp',  
           'veilleDaAvp',
            )
     query = ( 'SELECT  *  FROM perception_prestation;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("perception_prestation en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("perception_prestation fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('relanceavpsfm')




     collection = db.relanceavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'agent_avp_id',
           'commentaireImAvp',
           'dateRelanceImAvp',
            )
     query = ( 'SELECT  *  FROM relanceavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("relanceavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("relanceavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('suspensionavpsfm')




     collection = db.suspensionavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'date_suspension',
           'date_reprise',
            )
     query = ( 'SELECT  *  FROM suspensionavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("suspensionavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("suspensionavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('tracabiliteavpsfm')




     collection = db.tracabiliteavpsfm

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'actionDaAvp',
           'dateModifImAvp',
           'user_avp_id',
            )
     query = ( 'SELECT  *  FROM tracabiliteavpsfm;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("tracabiliteavpsfm en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("tracabiliteavpsfm fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('typenome')




     collection = db.typenome

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'CODE_STR',          
           'NOM_STR',
            )
     query = ( 'SELECT  *  FROM typenome;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("typenome en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("typenome fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('user')




     collection = db.user

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'enabled',              
           'salt',
           'last_login',
           'confirmation_token',
           'password_requested_at',
           'roles',
           'is_active',
           'created_at',
           'updated_at',
           'path',
           'is_deleted',
           'group_id',
           'source',
           'bureau_avp_id',
           'is_adherent',
           'is_api',
           'is_agent_commercial',
           'ouvrir_caisse',
           'hierarchie_agents',
            )
     query = ( 'SELECT  *  FROM user;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("user en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("user fini!")           
     cursor.close()
     cnx.close()
     
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('valeurchamp')




     collection = db.valeurchamp

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'cha_code_str',          
           'cgachamptypenome_id',              
           'cgacodenomenclature_id',
           'VALEUR',
           'ENABLED',
           'CODE_STR',
            )
     query = ( 'SELECT  *  FROM valeurchamp;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("valeurchamp en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("valeurchamp fini!")           
     cursor.close()
     cnx.close()
     
     client     = MongoClient()
     db         = client.SFM
     db.drop_collection('versement_cotisation')




     collection = db.versement_cotisation

     cnx        = pymysql.connect(host='127.0.0.1',
                                 port=3306,
                                 user='root',
                                 password='root',                             
                                 db='newschema',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
     cursor     = cnx.cursor()
     row_titles = (
           'id',          
           'adherent_avp_id',              
           'residentDaAvp',
           'regionDaAvp',
           'districtDaAvp',
           'bureauDaAvp',
           'paysDaAvp',
           'villeDaAvp',
            )
     query = ( 'SELECT  *  FROM versement_cotisation;' )
    
    
    
    #cursor.execute( query )
     cursor.execute( query )
    #mongo client specifically requires python dict
     cus = dict()
    #custom record id rather than mongodb default hash id                                          
     nbr = 1 
     print("versement_cotisation en cours...")                                               
    #print(len(list(cursor)))
    #cycle through each mySQL row
     for  ( row )  in cursor:
         
         nbr+=1
         
         cus['_id'] = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
                             
        
        #check if current row is null
         for i in range( 0, len( row_titles ) ):
              
              
              if  row[row_titles[i]] == None :
                 
                 #if the record is null, skip it                  
                  continue
              else:
                #conversion to string
                  row_title      = "".join( row_titles[i] )  
                #conversion to string 
                  field          = str( row[row_title] ) 
                
                #add current record's field's title and value             
                  cus[row_title] = field
                
                #we've completed processing this row, insert it into mongoldb      
         collection.insert_one( cus ).inserted_id
                
                #just for debug purposes, show me running row count
                #  print (id) 
                
                #shut down the mysql connection
     print("versement_cotisation fini!") 
     print("fin.")          
     cursor.close()
     cnx.close()
    
 
schedule.every().day.at("20:50").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)