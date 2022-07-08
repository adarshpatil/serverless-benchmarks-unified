import logging
import ast
import time
import nltk
nltk.data.path.append('nltk_data/')
from nltk.tokenize import word_tokenize
from util import *
import uuid
import hashlib

import pickle

################## IDENTIFYPHI

def extract_entities_from_message(message):
    # Read message response from file (instead of calling comprehendmedical)
    file = open("comprehendmedical_output-big","r")
    return ast.literal_eval(file.read())

def identifyphi(event_pickle):
    ### disaggr get begin
    event = pickle.loads(event_pickle)
    message = event['body']['message']
    ### disaggr get end

    ### compute begin
    start = time.time()
    entities_response = extract_entities_from_message(message)
    event['body']['entities'] = entities_response['Entities']
    print(time.time() - start)
    ### compute end

    ## disaggr put begin
    event_entities = pickle.dumps(event)
    ### disaggr put end

    return event_entities


################## DEIDENTIFY

def hash_message(message, entity_map):
    hashed_message = hashlib.sha3_256(message.encode()).hexdigest()
    return hashed_message

def deidentify_entities_in_message(message, entity_list):
    entity_map = dict()
    for entity in entity_list:
        salted_entity = entity['Text'] + str(uuid.uuid4())
        hashkey = hashlib.sha3_256(salted_entity.encode()).hexdigest()
        entity_map[hashkey] = entity['Text']
        message = message.replace(entity['Text'], hashkey)
    return message, entity_map

def deidentify(event_pickle):
    ### disaggr get begin
    event = pickle.loads(event_pickle)
    message = event['body']['message']
    entity_list = event['body']['entities']
    ### disaggr get end

    ### compute begin
    start = time.time()
    deidentified_message, entity_map = deidentify_entities_in_message(message, entity_list)
    hashed_message = hash_message(deidentified_message, entity_map)
    print(time.time() - start)
    ### compute end

    ### disaggr put begin
    deid_message = {"deid_message": deidentified_message, "hashed_message": hashed_message }
    deid_pickle = pickle.dumps(deid_message)
    ### disaggr put end

    return deid_pickle

################## ANONYMIZE


def mask_entities_in_message(message, entity_list):
    for entity in entity_list:
        message = message.replace(entity['Text'], '#' * len(entity['Text']))
    return message

def anonymize(event_pickle):
    ### disaggr get begin
    event = pickle.loads(event_pickle)
    message = event['body']['message']
    entity_list = event['body']['entities']
    ### disaggr get end

    ### compute begin
    start = time.time()
    for entity in entity_list:
        message = message.replace(entity['Text'], '#' * len(entity['Text']))
    print(time.time() - start)
    ### compute end

    ### disaggr put begin
    anon_msg_pickle = pickle.dumps(message)
    ### disaggr put end

    return anon_msg_pickle


################## ANALYTICS
def analytics(anon_msg_pickle):
    ### disaggr get begin
    message = pickle.loads(anon_msg_pickle)
    ### disaggr get end

    ### compute begin
    start = time.time()
    tokens = word_tokenize(message)
    print(time.time() - start)
    ### compute end

    ### disaggr put begin
    response = {'statusCode': 200, "body":tokens}
    response_pickle = pickle.dumps(response)
    ### disaggr put end

    return response_pickle


################## MAIN

print("********IDENTIFYPHI")

event = { "body":{
        "message": "RE: EXAMINEE : Abc DATE OF INJURY : October 4, 2000 DATE OF EXAMINATION : September 5, 2003 EXAMINING PHYSICIAN : X Y, MD Prior to the beginning of the examination, it is explained to the examinee that this examination is intended for evaluative purposes only, and that it is not intended to constitute a general medical examination. It is explained to the examinee that the traditional doctor-patient relationship does not apply to this examination, and that a written report will be provided to the agency requesting this examination. It has also been emphasized to the examinee that he should not attempt any physical activity beyond his tolerance, in order to avoid injury. CHIEF COMPLAINT: Aching and mid back pain.HISTORY OF PRESENT INJURY: Based upon the examinee's perspective:Mr. Abc is a 52-year-old self-employed, independent consultant for DEMILEE-USA. He is also a mechanical engineer. He reports that he was injured in a motor vehicle accident that occurred in October 4, 2000. At that time, he was employed as a purchasing agent for IBIKEN-USA. On the date of the motor vehicle accident, he was sitting in the right front passenger's seat, wearing seat and shoulder belt safety harnesses, in his 1996 or 1997 Volvo 850 Wagon automobile driven by his son. The vehicle was completely stopped and was \"slammed from behind\" by a van. The police officer, who responded to the accident, told Mr. Abc that the van was probably traveling at approximately 30 miles per hour at the time of impact.During the impact, Mr. Abc was restrained in the seat and did not contact the interior surface of the vehicle. He experienced immediate mid back pain. He states that the Volvo automobile sustained approximately $4600 in damage.He was transported by an ambulance, secured by a cervical collar and backboard to the emergency department. An x-ray of the whole spine was obtained, and he was evaluated by a physician's assistant. He was told that it would be \"okay to walk.\" He was prescribed pain pills and told to return for reevaluation if he experienced increasing pain.He returned to the Kaiser facility a few days later, and physical therapy was prescribed. Mr. Abc states that he was told that \"these things can take a long time.\" He indicates that after one year he was no better. He then states that after two years he was no better and worried if the condition would never get better.He indicates he saw an independent physician, a general practitioner, and an MRI was ordered. The MRI study was completed at ABCD Hospital. Subsequently, Mr. Abc returned and was evaluated by a physiatrist. The physiatrist reexamined the original thoracic spine x-rays that were taken on October 4, 2000, and stated that he did not know why the radiologist did not originally observe vertebral compression fractures. Mr. Abc believes that he was told by the physiatrist that it involved either T6-T7 or T7-T8.Mr. Abc reports that the physiatrist told him that little could be done besides participation in core strengthening. Mr. Abc describes his current exercise regimen, consisting of cycling, and it was deemed to be adequate. He was told, however, by the physiatrist that he could also try a Pilates type of core exercise program.The physiatrist ordered a bone scan, and Mr. Abc is unsure of the results. He does not have a formal follow up scheduled with Kaiser, and is awaiting re-contact by the physiatrist.He denies any previous history of symptomatology or injuries involving his back.CURRENT SYMPTOMS: He reports that he has the same mid back pain that has been present since the original injury. It is located in the same area, the mid thoracic spine area. It is described as a pain and an ache and ranges from 3/10 to 6/10 in intensity, and the intensity varies, seeming to go in cycles. The pain has been staying constant.When I asked whether or not the pain have improved, he stated that he was unable to determine whether or not he had experienced improvement. He indicates that there may be less pain, or conversely, that he may have developed more of a tolerance for the pain. He further states that \"I can power through it.\" \"I have learned how to manage the pain, using exercise, stretching, and diversion techniques.\" It is primary limitation with regards to the back pain involves prolonged sitting. After approximately two hours of sitting, he has required to get up and move around, which results in diminishment of the pain. He indicates that prior to the motor vehicle accident, he could sit for significantly longer periods of time, 10 to 12 hours on a regular basis, and up to 20 hours, continuously, on an occasional basis.He has never experienced radiation of the pain from the mid thoracic spine, and he has never experienced radicular symptoms of radiation of pain into the extremities, numbness, tingling, or weakness.Again, aggravating activities include prolonged sitting, greater than approximately two hours.Alleviating activities include moving around, stretching, and exercising. Also, if he takes ibuprofen, it does seem to help with the back pain.He is not currently taking medications regularly, but list that he takes occasional ibuprofen when the pain is too persistent.He indicates that he received several physical therapy sessions for treatment, and was instructed in stretching and exercises. He has subsequently performed the prescribed stretching and exercises daily, for nearly three years.With regards to recreational activities, he states that he has not limited his activities due to his back pain.He denies bowel or bladder dysfunction.FILES REVIEW:October 4, 2000: An ambulance was dispatched to the scene of a motor vehicle accident on South and Partlow Road. The EMS crew arrived to find a 49-year-old male sitting in the front passenger seat of a vehicle that was damaged in a rear-end collision and appeared to have minimal damage. He was wearing a seatbelt and he denied loss of consciousness. He also denied a pertinent past medical history. They noted pain in the lower cervical area, mid thoracic and lumbar area. They placed him on a backboard and transported him to Medical Center.October 4, 2000: He was seen in the emergency department of Medical Center. The provider is described as \"unknown.\" The history from the patient was that he was the passenger in the front seat of a car that was stopped and rear-ended. He stated that he did not exit the car because of pain in his upper back. He reported he had been wearing the seatbelt and harness at that time. He denied a history of back or neck injuries. He was examined on a board and had a cervical collar in place. He was complaining of mid back pain. He denied extremity weakness. Sensory examination was intact. There was no tenderness with palpation or flexion in the neck. The back was a little tender in the upper thoracic spine area without visible deformity. There were no marks on the back. His x-ray was described as \"no acute bony process.\" Listed visit diagnosis was a sprain-thoracic, and he was prescribed hydrocodone/acetaminophen tablets and Motrin 800 mg tablets.October 4, 2000: During the visit, a Clinician's Report of Disability document was signed by Dr. M, authorizing time loss from October 4, 2000, through October 8, 2000. The document also advised no heavy lifting, pushing, pulling, or overhead work for two weeks. During this visit, a thoracic spine x-ray series, two views, was obtained and read by Dr. JR. The findings demonstrate no evidence of acute injury. No notable arthritic findings. The pedicles and paravertebral soft tissues appear unremarkable.November 21, 2000: An outpatient progress note was completed at Kaiser, and the clinician of record was Dr. H. The history obtained documents that Mr. Abc continued to experience the same pain that he first noted after the accident, described as a discomfort in the mid thoracic spine area. It was non-radiating and described as a tightness. He also reported that he was hearing clicking noises that he had not previously heard. He denied loss of strength in the arms. The physical examination revealed good strength and normal deep tendon reflexes in the arms. There was minimal tenderness over T4 through T8, in an approximate area. The visit diagnosis listed was back pain. Also described in the assessment was residual pain from MVA, suspected bruised muscles. He was prescribed Motrin 800 mg tablets and an order was sent to physical therapy. Dr. N also documents that if the prescribed treatment measures were not effective, then he would suggest a referral to a physiatrist. Also, the doctor wanted him to discuss with physical therapy whether or not they thought that a chiropractor would be beneficial.December 4, 2000: He was seen at Kaiser for a physical therapy visit by Philippe Justel, physical therapist. The history obtained from Mr. Abc is that he was not improving. Symptoms described were located in the mid back, centrally. The examination revealed mild tenderness, centrally at T3-T8, with very poor segmental mobility. The posture was described as rigid T/S in flexion. Range of motion was described as within normal limits, without pain at the cervical spine and thoracic spine. The plan listed included two visits per week for two weeks, for mobilization. It is also noted that the physical therapist would contact the MD regarding a referral to a chiropractor.December 8, 2000: He was seen at Kaiser for a physical therapy visit by Mr. Justel. It was noted that the subjective category of the document revealed that there was no real change. It was noted that Mr. Abc tolerated the treatment well and that he was to see a chiropractor on Monday.December 11, 2000: He presented to the Chiropractic Wellness Center. There is a form titled 'Chiropractic Case History,' and it documents that Mr. Abc was involved in a motor vehicle accident, in which he was rear-ended in October. He has had mid back pain since that time. The pain is worsened with sitting, especially at a computer. The pain decreases when he changes positions, and sometimes when he walks. Mr. Abc reports that he occasionally takes 800 mg doses of ibuprofen. He reported he went to physical therapy treatment on two occasions, which helped for a few hours only. He did report that he had a previous history of transitory low back pain.During the visit, he completed a modified Oswestry Disability Questionnaire, and a WC/PI Subjective Complaint Form. He listed complaints of mid and low back pain of a sore and aching character. He rated the pain at grade 3-5/10, in intensity. He reported difficulty with sitting at a table, bending forward, or stooping. He reported that the pain was moderate and comes and goes.During the visit at the Chiropractic Wellness Center, a spinal examination form was completed. It documents palpation tenderness in the cervical, thoracic, and lumbar spine area and also palpation tenderness present in the suboccipital area, scalenes, and trapezia. Active cervical range of motion measured with goniometry reveals pain and restriction in all planes. Active thoracic range of motion measured with inclinometry revealed pain and restriction in rotation bilaterally. Active lumbosacral range of motion measured with inclinometry reveals pain with lumbar extension, right lateral flexion, and left lateral flexion.December 11, 2000: He received chiropractic manipulation treatment, and he was advised to return for further treatment at a frequency of twice a week.December 13, 2000: He returned to the Chiropractic Wellness Center to see Joe Smith, DC, and it is documented that his middle back was better.December 13, 2000: A personal injury patient history form is completed at the Chiropractic Wellness Center. Mr. Abc reported that on October 4, 2000, he was driving his 1996 Volvo 850 vehicle, wearing seat and shoulder belt safety harnesses, and completely stopped. He was rear-ended by a vehicle traveling at approximately 30 miles per hour. The impact threw him back into his seat, and he felt back pain and determined that it was not wise to move about. He reported approximate damage to his vehicle of $4800. He reported continuing mid and low back pain, of a dull and semi-intense nature. He reported that he was an export company manager for IBIKEN-USA, and that he missed two full days of work, and missed 10-plus partial days of work. He stated that he was treated initially after the motor vehicle accident at Kaiser and received painkillers and ibuprofen, which relieved the pain temporarily. He specifically denied ever experiencing similar symptoms.December 26, 2000: A no-show was documented at the Chiropractic Wellness Center.April 5, 2001: He received treatment at the Chiropractic Wellness Center. He reported that two weeks previously, his mid back pain had worsened.April 12, 2001: He received chiropractic treatment at the Chiropractic Wellness Center.April 16, 2001: He did not show up for his chiropractic treatment.April 19, 2001: He did not show up for his chiropractic treatment.April 26, 2001: He received chiropractic manipulation treatment at the Chiropractic Wellness Center. He reported that his mid back pain increased with sitting at the computer. At the conclusion of this visit, he was advised to return to the clinic as needed.September 6, 2002: An MRI of the thoracic spine was completed at ABCD Hospital and read by Dr. RL, radiologist. Dr. D noted the presence of minor anterior compression of some mid thoracic vertebrae of indeterminate age, resulting in some increased kyphosis. Some of the mid thoracic discs demonstrate findings consistent with degenerative disc disease, without a significant posterior disc bulging or disc herniation. There are some vertebral end-plate abnormalities, consistent with small Schmorl's nodes, one on the superior aspect of T7, which is compressed anteriorly, and on the inferior aspect of T6.May 12, 2003: He was seen at the Outpatient Clinic by Dr. L, internal medicine specialist. He was there for a health screening examination, and listed that his only complaints are for psoriasis and chronic mid back pain, which have been present since a 2000 motor vehicle accident. Mr. Abc reported that an outside MRI showed compression fractures in the thoracic spine. The history further documents that Mr. Abc is an avid skier and volunteers on the ski patrol. The physical examination revealed that he was a middle-aged Caucasian male in no acute distress. The diagnosis listed from this visit is back pain and psoriasis. Dr. L documented that he spent one hour in the examination room with the patient discussing what was realistic and reasonable with regard to screening testing. Dr. L also stated that since Mr. Abc was experiencing chronic back pain, he advised him to see a physiatrist for evaluation. He was instructed to bring the MRI to the visit with that practitioner.June 10, 2003: He was seen at the Physiatry Clinic by Dr. R, physiatrist. The complaint listed is mid back pain. In the subjective portion of the chart note, Dr. R notes that Mr. Abc is involved in the import/export business, and that he is physically active in cycling, skiing, and gardening. He is referred by Dr. L because of persistent lower thoracic pain, following a motor vehicle accident, on October 4, 2000. Mr. Abc told Dr. R that he was the restrained passenger of a vehicle that was rear-ended at a moderate speed. He stated that he experienced immediate discomfort in his thoracic spine area without radiation. He further stated that thoracic spine x-rays were obtained at the Sunnyside Emergency Room and read as normal. It is noted that Mr. Abc was treated conservatively and then referred to physical therapy where he had a number of visits in late of 2002 and early 2003. No further chart entries were documented about the back problem until Mr. Abc complained to Dr. L that he still had ongoing thoracic spine pain during a visit the previous month. He obtained an MRI, out of pocket, at ABCD Hospital and stated that he paid $1100 for it. Dr. R asked to see the MRI and was told by Mr. Abc that he would have to reimburse or pay him $1100 first. He then told the doctor that the interpretation was that he had a T7 and T8 compression fracture. Mr. Abc reported his improvement at about 20%, compared to how he felt immediately after the accident. He described that his only symptoms are an aching pain that occurs after sitting for four to five hours. If he takes a break from sitting and walks around, his symptoms resolve. He is noted to be able to bike, ski, and be active in his garden without any symptoms at all. He denied upper extremity radicular symptoms. He denied lower extremity weakness or discoordination. He also denied bowel or bladder control or sensation issues. Dr. R noted that Mr. Abc was hostile about the Kaiser health plan and was quite uncommunicative, only reluctantly revealing his history. The physical examination revealed that he moved about the examination room without difficulty and exhibited normal lumbosacral range of motion. There was normal thoracic spine motion with good chest expansion. Neurovascular examination of the upper extremities was recorded as normal. There was no spasticity in the lower extremities. There was no tenderness to palpation or percussion up and down the thoracic spine. Dr. R reviewed the thoracic spine films and noted the presence of \"a little compression of what appears to be T7 and T8 on the lateral view.\" Dr. R observed that this was not noted on the original x-ray interpretation. He further stated that the MRI, as noted above, was not available for review. Dr. R assessed that Mr. Abc was experiencing minimal thoracic spine complaints that probably related to the motor vehicle accident three years previously. The doctor further stated that \"the patient's symptoms are so mild as to almost not warrant intervention.\" He discussed the need to make sure that Mr. Abc's workstation was ergonomic and that Mr. Abc could pursue core strengthening. He further recommended that Mr. Abc look into participation in a Pilates class. Mr. Abc was insistent, so Dr. R made plans to order a bone scan to further discriminate the etiology of his symptoms. He advised Mr. Abc that the bone scan results would probably not change treatment. As a result of this visit, Dr. R diagnosed thoracic spine pain (724.1) and ordered a bone scan study.August 4, 2003: A whole body bone scan was completed and read by Karen P. Lewison. A moderate uptake was identified in the left mandible, and thought to be odontogenic in origin. Some uptake was identified in both shoulders, sternal and mandibular joints, hips, patellae, and in the right proximal tibia. These areas of uptake were thought to be arthritic and/or posttraumatic in origin. A very subtle dextroscoliosis was noted in the lower thoracic spine, with an even milder compensatory levoscoliosis noted at the thoracolumbar junction level. The findings were described as chronic in appearance and nonspecific, and not necessarily posttraumatic in origin.REVIEW OF SYSTEMS: Negative for complaints regarding the cardiovascular, pulmonary, renal, gastrointestinal, or psychiatric systems.PAST MEDICAL HISTORY: Prior Injuries:1. Age 16, leg fracture due to skiing.2. Achilles tendon tear, age 45.3. Rotator cuff injury due to skiing event, at age 42.ILLNESSES: Psoriasis.SURGERIES: Varicocele surgery.MEDICATIONS:1. Topical medications for psoriasis.2. Ibuprofen on an as needed basis for persistent mid back pain.ALLERGIES: No known drug allergies.SOCIO-ECONOMIC HISTORY:Marital Status: The patient is married with one dependent child.Highest Level of Education: He completed high school and also has received a Bachelor of Science in Mechanical Engineering.Tobacco Use: None.Alcohol Use: He consumes approximately one to two glasses of wine or beer per week.RECREATIONAL DRUG USE: None."

} }

event_message = pickle.dumps(event)

event_entities = identifyphi(event_message)

### DEIDENTIFY and ANONYMIZE+ANALYTICS can run in parallel

print("********DEIDENTIFY")
deid_msg = deidentify(event_entities)

print("********ANONYMIZE")
anon_msg = anonymize(event_entities)

print("********ANALYTICS")
response = analytics(anon_msg)
