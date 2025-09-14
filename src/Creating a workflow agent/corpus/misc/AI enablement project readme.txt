

Goal of the project: to implement AI throughout the Data Engg. SDLC i.e., from reguirements gathering till the data engg. project execution and getting results
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
****GENERATING REQUIREMENTS SPECIFICATION DOC****

These are the step we followed:

1. Write a prompt to generate a trasncript of communication between the client, USAAGM (client) and a team of consultants from Icube Consultancy Services Inc.
   
   Input: 2 text files containing sample transcripts
   
   Output: Transcript content
   
   Transcript showed the raw, realistic communication between the two parties with explanation of whats needed by client, consulatants identifying gaps and stating 
   what they could offer, the workflow and plan etc.
   Note: communication was the perspective of a Data Engg. project

   AI used: GPT 5

   Prompt: "Generate a transcript (refer to the attached files as templates) such that: 
		    imagine my company, Icube Consultancy Services Inc. (team could be data engineers or leads etc.) is at the client location right now to learn from the cleint about the 
			requirements of the project, ask questions/doubts to understand better, talk about the resources they have eg. the data engineers etc.
			
			Also, think about what more conversations would normally happen, what more questions might be asked, what gaps might be highlighted, what feasibility aspects will be discussed, 
			what roles and responsibilities will be talked about and much more. 
			
			Based on this, generate a realistic transcript like how a real requirements gathering interaction with a client would happen."


2. Write a prompt to use the transcript as well as a business requirement specification template as inputs to generate a professional, impressive requirements specification doc.
   Note: the idea was to generate the requirements specification doc for a Data Engg. project

   Input: Text file (containing all the transcript content) + BRS template
   
   Output: .md file i.e., Requirements Specification doc

   AI used: Claude sonnet 4

   Prompt used: "Read and understand the transcript attached, it comes from an interaction between the client and consultants as part of requirements gathering. 
			     
				 Now, using a template attached (brs_templat(1)), build a professional, impressive requirements specification doc that includes all the important aspects 
				 in a requirements specifications doc pertaining to a Data Engg project. Pfa the following doc: transcript and the template for the requirements specification doc. 
				 
				 Note: Should be able to fill in all the details mentioned in the template. The template I provided was for a full stack project, give me the 
				 requirements specification from a data engg. project perspective."

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

****GENERATING DESIGN DOC****

Input: Requirements Specifications doc

AI used: Claude sonnet 4

Prompt 1: "Use the requirements specifications document attached, review and analyze it thoroughly. Then, generate a professional, impressive design document for our 
		   data engg. project."

Prompt 2: "Note: we did not specify how much time it will take to accomplish all this. The goal is to complete this fully in the next 8 days or so. 
		   Also, not always do we have raw data stored in the broze layer. In some cases in the USAGM project, we have raw data being pulled from the API 
		   (in a Databricks env) followed by processing, transforming and storing the resulting data in a Bronze/Silver layer in ADLS. This data is then 
		   exported to Azure Synapse by the use of pipelines in Azure Data Factory.
		   So, considering the above points, generate a better, even more impressive professional design document."


Prompt 3: "Consider the following feedback and re-generate a refined, more prefessional and impressive design doc: 
		   1. We do need the use of a Dynamic Storage Router. 
		   2. No code in the doc."

Tips: No mention stuff like 8 daya plan, 10 day plan, rapid implementation plan etc. rephrase to something else thats more professional and impressive.


then we were able to use Lucid charts to create the architecture diagram

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

****GENERATING TEST CASES****

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

****BUILD PHASE****


followed by this was developmebt:

1. got github copilot, integrated that with vs code and databricks such that any file or code is on vs code (in local system's path), it will auto sync and reflect on databricks
2. identifided tw oworkflows in data engg. i.e., structured (emplifi profile metrics) and unstructured (semrush)
3. with the help of prompting and github copilot, we were able to generate code for databricks ntbk (for data processing, accessing aldls, creating medallion layers, creating cponnection
	to azure synapse db and gather the top urls (in case of semrush), also create table schemas in syanpse etc.)
4. with the help of extensive prompoting and copilot, we were able to sync guthub repo wiuth adf and then generate using copilot, the json scripts for 
	datasets (synapse, adls), linked services (databricks, adls, synapse), pipeline (big json script) 

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------







---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


























