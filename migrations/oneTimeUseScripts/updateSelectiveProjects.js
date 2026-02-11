const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../../") + "/.env" });
const { MongoClient, ObjectId } = require("mongodb");
const mongo_url = process.env.MONGODB_URL;
if (!mongo_url) {
  console.error("❌ MONGODB_URL not set");
  process.exit(1);
}

let bulkOps = [
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "6936af09c881b00007198ecf",
          "programExternalId": "DIET_MUZAFFARPUR _08_12_2025",
          "solutionId": "6936afcdfce8c80008d5780e",
          "solutionExternalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "6936af09c881b00007198ecf",
            "externalId": "DIET_MUZAFFARPUR _08_12_2025",
            "description": "यह MIP शिक्षकों को इस दिशा में सहयोग करता है कि वे विद्यार्थियों को आयु-उपयुक्त अज्ञात सरल पाठ जिसमे 6-8 वाक्य हों को स्पष्टता से पढ़ने और प्रवाह के साथ बोलने में सक्षम बना सकें। इसका उद्देश्य नियमित अभ्यास के माध्यम से छात्रों की पठन क्षमता में सुधार लाना है।",
            "name": "DIET MUZAFFARPUR _DEP_2025-26",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6936afcdfce8c80008d5780e",
            "externalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
            "description": "इस MIP को क्रियान्वित कर शिक्षक प्रवाहपूर्ण एवं अर्थपूर्ण पठन से जुड़ी गतिविधियों का संचालन कर बच्चों में पठन के प्रति रुचि विकसित करने तथा उनके पठन कौशल को सशक्त बनाने में सक्षम होंगे।",
            "name": "BH_DIET_Muzaffarpur_MIP_प्रवाहपूर्ण और अर्थपूर्ण पठन कौशल विकास",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task1-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99535"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task1-1765191628832-1765191629953"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task2-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99538"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task2-1765191628832-1765191629956"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task3-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task3-1765191628832-1765191629959"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task4-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task4-1765191628832-1765191629961"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task5-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99541"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task5-1765191628832-1765191629964"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task6-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99544"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task6-1765191628832-1765191629966"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task7-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99547"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task7-1765191628832-1765191629969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6964ac78e43d2d000853ee37",
        "userId": "0332bc28-54af-411d-955c-ecf6320c8acf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task8-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9954a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task8-1765191628832-1765191629971"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6909e06638c28700089fa7a6",
          "solutionExternalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6909e06638c28700089fa7a6",
            "externalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.7",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c912"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1762254949974-1762254950355"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c915"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1762254949974-1762254950358"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c918"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1762254949974-1762254950361"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1762254949974-1762254950364"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1762254949974-1762254950367"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c921"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1762254949974-1762254950370"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c924"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1762254949974-1762254950373"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c927"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1762254949974-1762254950375"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692414d52462c200088af4d2",
        "userId": "0d56e6bb-6e43-4563-be66-9567642b8e96"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c92a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1762254949974-1762254950378"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "6936af09c881b00007198ecf",
          "programExternalId": "DIET_MUZAFFARPUR _08_12_2025",
          "solutionId": "6936afcdfce8c80008d5780e",
          "solutionExternalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "6936af09c881b00007198ecf",
            "externalId": "DIET_MUZAFFARPUR _08_12_2025",
            "description": "यह MIP शिक्षकों को इस दिशा में सहयोग करता है कि वे विद्यार्थियों को आयु-उपयुक्त अज्ञात सरल पाठ जिसमे 6-8 वाक्य हों को स्पष्टता से पढ़ने और प्रवाह के साथ बोलने में सक्षम बना सकें। इसका उद्देश्य नियमित अभ्यास के माध्यम से छात्रों की पठन क्षमता में सुधार लाना है।",
            "name": "DIET MUZAFFARPUR _DEP_2025-26",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6936afcdfce8c80008d5780e",
            "externalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
            "description": "इस MIP को क्रियान्वित कर शिक्षक प्रवाहपूर्ण एवं अर्थपूर्ण पठन से जुड़ी गतिविधियों का संचालन कर बच्चों में पठन के प्रति रुचि विकसित करने तथा उनके पठन कौशल को सशक्त बनाने में सक्षम होंगे।",
            "name": "BH_DIET_Muzaffarpur_MIP_प्रवाहपूर्ण और अर्थपूर्ण पठन कौशल विकास",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task1-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99535"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task1-1765191628832-1765191629953"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task2-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99538"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task2-1765191628832-1765191629956"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task3-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task3-1765191628832-1765191629959"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task4-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task4-1765191628832-1765191629961"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task5-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99541"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task5-1765191628832-1765191629964"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task6-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99544"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task6-1765191628832-1765191629966"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task7-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99547"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task7-1765191628832-1765191629969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6969bf98d825ab00080b80f7",
        "userId": "56410bcb-5026-4b2a-98ae-be798210581f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task8-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9954a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task8-1765191628832-1765191629971"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6909e06638c28700089fa7a6",
          "solutionExternalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6909e06638c28700089fa7a6",
            "externalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.7",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c912"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1762254949974-1762254950355"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c915"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1762254949974-1762254950358"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c918"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1762254949974-1762254950361"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1762254949974-1762254950364"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1762254949974-1762254950367"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c921"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1762254949974-1762254950370"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c924"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1762254949974-1762254950373"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c927"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1762254949974-1762254950375"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "691ef75d2462c20008831aa1",
        "userId": "5c53a135-fb35-4f54-96ec-4d9611780edb"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c92a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1762254949974-1762254950378"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "685bd9ac3d8d030008fda1bb",
          "solutionExternalId": "BHPBLMIP25-1750849964565-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "685bd9ac3d8d030008fda1bb",
            "externalId": "BHPBLMIP25-1750849964565-PROJECT-SOLUTION",
            "description": "शिक्षक-शिक्षिका प्रोजेक्ट बेस्ड लर्निंग (PBL) शिक्षण पद्धति के माध्यम से छात्र-छात्राओं का रिवीजन कर पाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट - 3.2",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task1-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c079b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task1-1750849964565-1750849964830"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task2-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c079e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task2-1750849964565-1750849964833"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task3-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c07a1"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task3-1750849964565-1750849964835"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task4-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c07a4"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task4-1750849964565-1750849964838"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task5-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c07a7"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task5-1750849964565-1750849964840"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task6-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c07aa"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task6-1750849964565-1750849964842"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task7-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c07ad"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task7-1750849964565-1750849964845"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68656c002462c20008d68f37",
        "userId": "6001fc37-72e4-4bdb-9869-fe5049c92eaf"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "BHPBLMIP2-Task8-1750849964565",
          "tasks.$[task].referenceId": "685bd9ac3e090800082c07b0"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "BHPBLMIP2-Task8-1750849964565-1750849964847"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6909e06638c28700089fa7a6",
          "solutionExternalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6909e06638c28700089fa7a6",
            "externalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.7",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c912"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c915"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c918"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c921"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c924"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c927"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69256acf2462c200088f07a0",
        "userId": "849c944e-a316-4a63-8b9c-c738d93b705e"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c92a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "6936af09c881b00007198ecf",
          "programExternalId": "DIET_MUZAFFARPUR _08_12_2025",
          "solutionId": "6936afcdfce8c80008d5780e",
          "solutionExternalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "6936af09c881b00007198ecf",
            "externalId": "DIET_MUZAFFARPUR _08_12_2025",
            "description": "यह MIP शिक्षकों को इस दिशा में सहयोग करता है कि वे विद्यार्थियों को आयु-उपयुक्त अज्ञात सरल पाठ जिसमे 6-8 वाक्य हों को स्पष्टता से पढ़ने और प्रवाह के साथ बोलने में सक्षम बना सकें। इसका उद्देश्य नियमित अभ्यास के माध्यम से छात्रों की पठन क्षमता में सुधार लाना है।",
            "name": "DIET MUZAFFARPUR _DEP_2025-26",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6936afcdfce8c80008d5780e",
            "externalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
            "description": "इस MIP को क्रियान्वित कर शिक्षक प्रवाहपूर्ण एवं अर्थपूर्ण पठन से जुड़ी गतिविधियों का संचालन कर बच्चों में पठन के प्रति रुचि विकसित करने तथा उनके पठन कौशल को सशक्त बनाने में सक्षम होंगे।",
            "name": "BH_DIET_Muzaffarpur_MIP_प्रवाहपूर्ण और अर्थपूर्ण पठन कौशल विकास",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task1-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99535"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task1-1765191628832-1765191629953"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task2-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99538"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task2-1765191628832-1765191629956"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task3-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task3-1765191628832-1765191629959"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task4-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task4-1765191628832-1765191629961"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task5-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99541"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task5-1765191628832-1765191629964"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task6-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99544"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task6-1765191628832-1765191629966"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task7-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99547"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task7-1765191628832-1765191629969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "696483a8d16bbf0008f75890",
        "userId": "9567a493-c7f4-4c25-9cd4-e0186825b217"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task8-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9954a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task8-1765191628832-1765191629971"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6909e06638c28700089fa7a6",
          "solutionExternalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6909e06638c28700089fa7a6",
            "externalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.7",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c912"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1762254949974-1762254950355"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c915"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1762254949974-1762254950358"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c918"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1762254949974-1762254950361"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1762254949974-1762254950364"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1762254949974-1762254950367"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c921"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1762254949974-1762254950370"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c924"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1762254949974-1762254950373"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c927"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1762254949974-1762254950375"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6925415b2462c200088d824d",
        "userId": "ad5fe5d9-d9de-47d5-85e0-67c921853b95"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c92a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1762254949974-1762254950378"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "6936af09c881b00007198ecf",
          "programExternalId": "DIET_MUZAFFARPUR _08_12_2025",
          "solutionId": "6936afcdfce8c80008d5780e",
          "solutionExternalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "6936af09c881b00007198ecf",
            "externalId": "DIET_MUZAFFARPUR _08_12_2025",
            "description": "यह MIP शिक्षकों को इस दिशा में सहयोग करता है कि वे विद्यार्थियों को आयु-उपयुक्त अज्ञात सरल पाठ जिसमे 6-8 वाक्य हों को स्पष्टता से पढ़ने और प्रवाह के साथ बोलने में सक्षम बना सकें। इसका उद्देश्य नियमित अभ्यास के माध्यम से छात्रों की पठन क्षमता में सुधार लाना है।",
            "name": "DIET MUZAFFARPUR _DEP_2025-26",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6936afcdfce8c80008d5780e",
            "externalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
            "description": "इस MIP को क्रियान्वित कर शिक्षक प्रवाहपूर्ण एवं अर्थपूर्ण पठन से जुड़ी गतिविधियों का संचालन कर बच्चों में पठन के प्रति रुचि विकसित करने तथा उनके पठन कौशल को सशक्त बनाने में सक्षम होंगे।",
            "name": "BH_DIET_Muzaffarpur_MIP_प्रवाहपूर्ण और अर्थपूर्ण पठन कौशल विकास",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task1-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99535"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task1-1765191628832-1765191629953"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task2-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99538"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task2-1765191628832-1765191629956"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task3-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task3-1765191628832-1765191629959"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task4-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task4-1765191628832-1765191629961"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task5-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99541"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task5-1765191628832-1765191629964"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task6-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99544"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task6-1765191628832-1765191629966"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task7-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99547"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task7-1765191628832-1765191629969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69745d54d825ab000826667d",
        "userId": "be2572a6-b60f-421e-8810-d1fd766b2452"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task8-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9954a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task8-1765191628832-1765191629971"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "69318473d1d0b900080c8ed5",
          "solutionExternalId": "PBLMIP25A8-1764852851658-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "69318473d1d0b900080c8ed5",
            "externalId": "PBLMIP25A8-1764852851658-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.8",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc15"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1764852851658-1764852851969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc18"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1764852851658-1764852851973"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc1b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1764852851658-1764852851976"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc1e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1764852851658-1764852851979"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc21"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1764852851658-1764852851983"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc24"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1764852851658-1764852851987"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc27"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1764852851658-1764852851990"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc2a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1764852851658-1764852851993"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69393cca2462c20008ac854c",
        "userId": "c5e896e7-6050-4b01-863f-e4af757da4ce"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc2d"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1764852851658-1764852851996"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "6936af09c881b00007198ecf",
          "programExternalId": "DIET_MUZAFFARPUR _08_12_2025",
          "solutionId": "6936afcdfce8c80008d5780e",
          "solutionExternalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "6936af09c881b00007198ecf",
            "externalId": "DIET_MUZAFFARPUR _08_12_2025",
            "description": "यह MIP शिक्षकों को इस दिशा में सहयोग करता है कि वे विद्यार्थियों को आयु-उपयुक्त अज्ञात सरल पाठ जिसमे 6-8 वाक्य हों को स्पष्टता से पढ़ने और प्रवाह के साथ बोलने में सक्षम बना सकें। इसका उद्देश्य नियमित अभ्यास के माध्यम से छात्रों की पठन क्षमता में सुधार लाना है।",
            "name": "DIET MUZAFFARPUR _DEP_2025-26",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6936afcdfce8c80008d5780e",
            "externalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
            "description": "इस MIP को क्रियान्वित कर शिक्षक प्रवाहपूर्ण एवं अर्थपूर्ण पठन से जुड़ी गतिविधियों का संचालन कर बच्चों में पठन के प्रति रुचि विकसित करने तथा उनके पठन कौशल को सशक्त बनाने में सक्षम होंगे।",
            "name": "BH_DIET_Muzaffarpur_MIP_प्रवाहपूर्ण और अर्थपूर्ण पठन कौशल विकास",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task1-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99535"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task1-1765191628832-1765191629953"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task2-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99538"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task2-1765191628832-1765191629956"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task3-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task3-1765191628832-1765191629959"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task4-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task4-1765191628832-1765191629961"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task5-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99541"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task5-1765191628832-1765191629964"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task6-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99544"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task6-1765191628832-1765191629966"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task7-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99547"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task7-1765191628832-1765191629969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69648bc1d16bbf0008f79e16",
        "userId": "ca6ce26a-5cea-4729-b457-a932bffc2af7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task8-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9954a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task8-1765191628832-1765191629971"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "6936af09c881b00007198ecf",
          "programExternalId": "DIET_MUZAFFARPUR _08_12_2025",
          "solutionId": "6936afcdfce8c80008d5780e",
          "solutionExternalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "6936af09c881b00007198ecf",
            "externalId": "DIET_MUZAFFARPUR _08_12_2025",
            "description": "यह MIP शिक्षकों को इस दिशा में सहयोग करता है कि वे विद्यार्थियों को आयु-उपयुक्त अज्ञात सरल पाठ जिसमे 6-8 वाक्य हों को स्पष्टता से पढ़ने और प्रवाह के साथ बोलने में सक्षम बना सकें। इसका उद्देश्य नियमित अभ्यास के माध्यम से छात्रों की पठन क्षमता में सुधार लाना है।",
            "name": "DIET MUZAFFARPUR _DEP_2025-26",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6936afcdfce8c80008d5780e",
            "externalId": "MIPMFP12-1765191628832-PROJECT-SOLUTION",
            "description": "इस MIP को क्रियान्वित कर शिक्षक प्रवाहपूर्ण एवं अर्थपूर्ण पठन से जुड़ी गतिविधियों का संचालन कर बच्चों में पठन के प्रति रुचि विकसित करने तथा उनके पठन कौशल को सशक्त बनाने में सक्षम होंगे।",
            "name": "BH_DIET_Muzaffarpur_MIP_प्रवाहपूर्ण और अर्थपूर्ण पठन कौशल विकास",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task1-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99535"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task1-1765191628832-1765191629953"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task2-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99538"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task2-1765191628832-1765191629956"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task3-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task3-1765191628832-1765191629959"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task4-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9953e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task4-1765191628832-1765191629961"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task5-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99541"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task5-1765191628832-1765191629964"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task6-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99544"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task6-1765191628832-1765191629966"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task7-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a99547"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task7-1765191628832-1765191629969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "69647ba9d16bbf0008f71e54",
        "userId": "d3022c9a-8d52-4db9-85eb-cad814a16d77"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "MIPMFP12-Task8-1765191628832",
          "tasks.$[task].referenceId": "6936afcd2462c20008a9954a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "MIPMFP12-Task8-1765191628832-1765191629971"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "689097585da94d0008bf055a",
          "solutionExternalId": "PBLMIP25A4-1754306392883-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "689097585da94d0008bf055a",
            "externalId": "PBLMIP25A4-1754306392883-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.4",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task1-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d6997"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task1-1754306392883-1754306392873"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task2-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d699a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task2-1754306392883-1754306392875"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task3-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d699d"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task3-1754306392883-1754306392878"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task4-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d69a1"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task4-1754306392883-1754306392880"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task5-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d69a4"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task5-1754306392883-1754306392883"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task6-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d69a7"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task6-1754306392883-1754306392885"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task7-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d69aa"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task7-1754306392883-1754306392888"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68b6aea12462c200084c470b",
        "userId": "d59d02f4-c92f-433e-9863-44f46be01b82"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A4-Task8-1754306392883",
          "tasks.$[task].referenceId": "689097582462c200080d69ad"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A4-Task8-1754306392883-1754306392891"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "69318473d1d0b900080c8ed5",
          "solutionExternalId": "PBLMIP25A8-1764852851658-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "69318473d1d0b900080c8ed5",
            "externalId": "PBLMIP25A8-1764852851658-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.8",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc15"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1764852851658-1764852851969"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc18"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1764852851658-1764852851973"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc1b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1764852851658-1764852851976"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc1e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1764852851658-1764852851979"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc21"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1764852851658-1764852851983"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc24"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1764852851658-1764852851987"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc27"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1764852851658-1764852851990"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc2a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1764852851658-1764852851993"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "6944e5d4930e840008e5d7c8",
        "userId": "d5e03056-a9fb-4485-8148-e6d6ac6caf53"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1764852851658",
          "tasks.$[task].referenceId": "693184732462c20008a4fc2d"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1764852851658-1764852851996"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6874f3985da94d0008a65150",
          "solutionExternalId": "PBLMIP25J3-1752495001356-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6874f3985da94d0008a65150",
            "externalId": "PBLMIP25J3-1752495001356-PROJECT-SOLUTION",
            "description": "शिक्षक-शिक्षिका प्रोजेक्ट बेस्ड लर्निंग (PBL) शिक्षण पद्धति के माध्यम से छात्र-छात्राओं का रिवीजन कर पाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.3",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task1-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f42"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task1-1752495001356-1752495001115"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task2-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f45"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task2-1752495001356-1752495001118"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task3-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f48"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task3-1752495001356-1752495001120"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task4-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f4b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task4-1752495001356-1752495001123"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task5-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f4e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task5-1752495001356-1752495001125"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task6-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f51"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task6-1752495001356-1752495001127"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task7-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f54"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task7-1752495001356-1752495001130"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "68954e992462c20008166522",
        "userId": "e4c78949-a3f9-438e-82fd-5b77a5cc2bca"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25J3-Task8-1752495001356",
          "tasks.$[task].referenceId": "6874f3982462c20008dd8f57"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25J3-Task8-1752495001356-1752495001133"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6909e06638c28700089fa7a6",
          "solutionExternalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6909e06638c28700089fa7a6",
            "externalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.7",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c912"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1762254949974-1762254950355"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c915"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1762254949974-1762254950358"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c918"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1762254949974-1762254950361"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1762254949974-1762254950364"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1762254949974-1762254950367"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c921"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1762254949974-1762254950370"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c924"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1762254949974-1762254950373"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c927"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1762254949974-1762254950375"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692413f52462c200088aeeaf",
        "userId": "fa9acb55-f544-4dc8-86da-5ce39d9d498f"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c92a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1762254949974-1762254950378"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "isAPrivateProgram": false,
          "isMigratedDueToReportIssue": true,
          "programId": "680893ff3d8d030008cd037a",
          "programExternalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
          "solutionId": "6909e06638c28700089fa7a6",
          "solutionExternalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
          "programInformation": {
            "_id": "680893ff3d8d030008cd037a",
            "externalId": "PGM_Bihar_Project Based_Learning_Program_Year3_599958",
            "description": "बिहार राज्य के विद्यालयों जहाँ 6 से 8 की कक्षाएँ संचालित होती हैं उनमें शिक्षक प्रोजेक्ट बेस्ड लर्निंग आधारित पाठ योजना का उपयोग कर विज्ञान की अवधारणाओं को पढाएंगेI बच्चे पाठ योजना से अवधारणाओं को भी जानेंगे एवं उससे सम्बंधित मॉडल भी तैयार करेंगेI",
            "name": "Bihar Project Based Learning Program Year 3",
            "isAPrivateProgram": false
          },
          "solutionInformation": {
            "_id": "6909e06638c28700089fa7a6",
            "externalId": "PBLMIP25A7-1762254949974-PROJECT-SOLUTION",
            "description": "शिक्षक प्रोजेक्ट बेस्ड लर्निंग पाठ योजना का उपयोग करके विज्ञान एवं गणित पढ़ाएंगे",
            "name": "BH_प्रोजेक्ट बेस्ड लर्निंग आधारित माइक्रो इम्प्रूवमेंट प्रोजेक्ट-3.7",
            "isAPrivateProgram": false
          }
        }
      }
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task1-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c912"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task1-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task2-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c915"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task2-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task3-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c918"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task3-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task4-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91b"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task4-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task5-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c91e"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task5-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task6-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c921"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task6-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task7-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c924"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task7-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task8-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c927"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task8-1762254949974"
        }
      ]
    }
  },
  {
    "updateOne": {
      "filter": {
        "_id": "692852d22462c200089882b2",
        "userId": "37e98899-6bf9-425d-993d-f48794968ff7"
      },
      "update": {
        "$set": {
          "tasks.$[task].externalId": "PBLMIP25A7-Task9-1762254949974",
          "tasks.$[task].referenceId": "6909e0662462c2000875c92a"
        }
      },
      "arrayFilters": [
        {
          "task.externalId": "PBLMIP25A7-Task9-1762254949974"
        }
      ]
    }
  }
]

function convertBulkOpsObjectIds(bulkOps) {
  if (!Array.isArray(bulkOps)) return bulkOps;

  for (const op of bulkOps) {
    if (!op.updateOne) continue;

    const { filter, update } = op.updateOne;

    // -------- FILTER _id --------
    if (filter?._id && typeof filter._id === "string") {
      filter._id = new ObjectId(filter._id);
    }

    const set = update?.$set;
    if (!set) continue;

    // -------- programId --------
    if (set.programId && typeof set.programId === "string") {
      set.programId = new ObjectId(set.programId);
    }

    // -------- solutionId --------
    if (set.solutionId && typeof set.solutionId === "string") {
      set.solutionId = new ObjectId(set.solutionId);
    }

    // -------- programInformation._id --------
    if (
      set.programInformation?._id &&
      typeof set.programInformation._id === "string"
    ) {
      set.programInformation._id = new ObjectId(
        set.programInformation._id
      );
    }

    // -------- solutionInformation._id --------
    if (
      set.solutionInformation?._id &&
      typeof set.solutionInformation._id === "string"
    ) {
      set.solutionInformation._id = new ObjectId(
        set.solutionInformation._id
      );
    }
  }

  return bulkOps;
}

async function run() {
  let connection;
  try{
    connection = await MongoClient.connect(mongo_url, {
      useNewUrlParser: true,
      useUnifiedTopology: true
    });
    const db = connection.db();

    if (!bulkOps || bulkOps.length === 0) {
      console.log("⚠️ No operations to execute.");
      return;
    }

    // converts string to ObjectIds
    bulkOps = convertBulkOpsObjectIds(bulkOps);

    // DB write
    const bulkWriteResult = await db.collection("projects").bulkWrite(bulkOps, {ordered : false});

    console.log("✅ Bulk write completed.");
    console.log("Matched:", bulkWriteResult.matchedCount);
    console.log("Modified:", bulkWriteResult.modifiedCount);
    console.log("Upserts:", bulkWriteResult.upsertedCount);

  }
  catch(error){
    console.error("Exiting script!!", error);
    process.exit(1);
  }
  finally {
    if (connection) {
      await connection.close();
      console.log("🔌 MongoDB connection closed.");
    }
  }
}

run();