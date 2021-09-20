import os
from elastic_enterprise_search import AppSearch
from tika import parser
from tika import language
import re
from elastic_enterprise_search import EnterpriseSearch

class PDFParser:
    def __init__(self, filename): 
        self.doc = {}
        self.split_doc = {}  # Used for subsections
        self.parsed_pdf = parser.from_file(filename)
        self.data = self.parsed_pdf['content']
        self.metadata = self.parsed_pdf['metadata']

        tika_server_status = self.parsed_pdf['status']
        #print(tika_server_status)  # TODO Do status test

    def parseFileMetadata(self):
        self.doc['date'] = self.metadata['date']
        self.doc['author'] = self.metadata['Author']
        self.doc['name'] = self.metadata['resourceName']
        self.doc['contenttype'] = self.metadata['Content-Type']
        self.doc['language'] = language.from_buffer(self.data)

    # TODO Split to section and subsection functions
    def parseText(self):
        # TODO cover usecase without AVSNITT in title
        # TODO use language specific regexps
        avsnitt = re.split('(?=\nAVSNITT|SECTION [0-9]{1,2}:)', self.data, flags = re.IGNORECASE)

        for i in range(0, len(avsnitt)):
            if(i==0): self.split_doc['header'] = avsnitt[i]

            avsnitt_title = re.split('((AVSNITT|SECTION) ([0-9]{1,2})[:|.](.*)\n)', avsnitt[i], flags=re.IGNORECASE)

            if(len(avsnitt_title) > 3 and avsnitt_title[3]==str(i)): # Validate if the parsing was somewhat correct
                self.doc['avsnitt'+str(i)] = avsnitt[i]
                self.split_doc['avsnitt' + str(i) + 'titel'] = avsnitt_title[4] # Validate if correct title from title list
                #print('"'+avsnitt_title[3]+'": "'+avsnitt_title[4].strip()+'"')

                # Split by subsections
                subavsnitt_regexp = '\n'+str(i)+'\.(?P<subnr>[0-9])[ |\.](?P<subname>.*)\n'
                #for line in avsnitt[i].splitlines():
                subavsnitt = re.split(subavsnitt_regexp, avsnitt[i], flags=re.IGNORECASE)
                for sub in range(1, len(subavsnitt), 3):
                    #print('"'+str(i)+'_'+subavsnitt[sub]+'": "' + subavsnitt[sub+1].strip() + '"')
                    self.split_doc['avsnitt'+str(i)+'_'+subavsnitt[sub]] = subavsnitt[sub+2]
                    self.split_doc['avsnitt'+str(i)+'_'+subavsnitt[sub]+'titel'] = subavsnitt[sub+1].strip()

        #print(self.doc)
        return self.doc

    def getAngivelser(self, pattern, data):
        return re.findall(pattern, data)

    def getFaroangivelser(self):
        farokod_pattern = re.compile(r'(H[0-9]{3})')
        faroangivelser = self.getAngivelser(farokod_pattern, self.split_doc["avsnitt2_2"])
        self.doc["faroangivelse"] = list(dict.fromkeys(faroangivelser))
        return faroangivelser

    def getSkyddsangivelser(self):
        sakkod_pattern = r'(P[0-9]{3})'
        skyddsangivelser = self.getAngivelser(sakkod_pattern, self.split_doc["avsnitt2_2"])
        self.doc["skyddsangivelser"] = list(dict.fromkeys(skyddsangivelser))
        return skyddsangivelser

    def getKategori(self):
        katkod_pattern = r'Kategori *([0-9])'
        kat_nrs = self.getAngivelser(katkod_pattern, self.split_doc["avsnitt2_1"])
        self.doc["kategori"] = list(kat_nrs)

    def getTitle(self):
        self.doc["produktbeteckning"] = self.split_doc["avsnitt1_1"].strip()
        return self.doc["produktbeteckning"]

    def getSupplyer(self):
        text_pattern = re.compile(r'([a-zA-Z]+)')
        for line in self.split_doc["avsnitt1_3"].splitlines():
            if(text_pattern.match(line)):
                self.doc["leverantor"] = line.strip()
                break
        return self.doc["leverantor"]


class ElasticCloud:
    def __init__(self):
        # Connecting to an instance on Elastic Cloud w/ username and password
        ent_search = EnterpriseSearch(
            "https://imvelo-befed5.ent.eu-central-1.aws.cloud.es.io",
            http_auth=("elastic", "zXiYaWXd2lywWtGH6qcyGPs4"),
        )
        print(ent_search.get_version())


class ElasticCloudAppSearch:
    def __init__(self):
        # Connecting to an instance on Elastic Cloud w/ an App Search private key
        self.app_search = AppSearch(
            "https://ea2b9e38cdb6410b8ac920c8bbe2831c.ent-search.eu-central-1.aws.cloud.es.io",
            http_auth="private-rhrakddnnoq2np1tfbpdvod2"
        )

    # print(app_search.get_version())
    # print(app_search.get("document_count", 1))
    def schema_get(self, index):
        resp = self.app_search.get_schema(engine_name=index)
        #print(resp)

    def index_doc(self, index, doc):
        return self.app_search.index_documents(engine_name=index, documents=[doc])
            # engine_name="faroangivelser",



def writeToFile(data, filename):
    with open(filename, mode='w', encoding='utf-8') as file_object:
        file_object.write(str(data))
        #print("data", file=file_object)
        #print(data, file=file_object)

def getAllPdfFiles():
    pdfFiles = []
    for filename in os.listdir('./referensdokument'):
        if filename.endswith('.pdf'):
            pdfFiles.append(filename)

    return pdfFiles.sort(key=str.lower)

def getParserDetails():
    from tika import config
    print(config.getParsers())
    print(config.getMimeTypes())
    print(config.getDetectors())



if __name__ == "__main__":
    filename = "LOCTITE 243 2020-06-23.pdf"
    fileparser = PDFParser("./referensdokument/"+filename)

    fileparser.parseFileMetadata()
    fileparser.parseText()

    fileparser.getTitle()
    fileparser.getSupplyer()
    fileparser.getKategori()
    fileparser.getFaroangivelser()
    fileparser.getSkyddsangivelser()

    indexWriter = ElasticCloudAppSearch()
    indexWriter.schema_get("imvelo-search")
    #field_limit_doc = dict(list(fileparser.split_doc.items())[:54])

    print(len(fileparser.doc))
    field_limit_doc = dict(list(fileparser.doc.items())[:50])
    print(len(field_limit_doc))
    result = indexWriter.index_doc("imvelo-search", field_limit_doc)
    print(result)

    #writeToFile(fileparser.doc, filename+"_doc.json")
    #writeToFile(fileparser.split_doc, filename + "_splitdoc.json")


# Referensdata
#doc = {'id': "LOCTITE243",
#       'produktbeteckning': "LOCTITE 243",
#       'produkttyp': "Lim",
#       'faroangivelse': ["H317", "H411"],
#       'skyddsangivelse': ["P101", "P102", "P501"],
#       'kategori': ["1", "2"],
#       'farliga_komponenter': ["2082-81-7", "101-37-1", "94108-97-1", "126098-16-6", "80-15-9", "114-83-0", "110-16-7", "130-15-4"],
#       'leverantor': "Henkel Norden AB",
#       'data': data}

#parsed_pdf = parser.from_file("./referensdokument/2K Plastlim Snabb 50 ml (A) 2020-03-26.pdf")
#data = parsed_pdf['content']
#doc = {'id': "2KPlastlimSnabb",
#       'produktbeteckning': "2K Plastlim Snabb",
#        'produkttyp': "Lim",
#       'faroangivelse': ["H315", "H317", "H319", "H332", "H334", "H335", "H351", "H373"],
#       'skyddsangivelse': ["P201", "P264", "P280"],
#       'kategori': ["1", "2", "3", "4"],
#       'farliga_komponenter': ["9016-87-9", "101-68-8", "25686-28-6", "53862-89-8", "9048-57-1", "Inte klassificerat", "57029-46-6", "52409-10-6", "14808-60-7"],
#       'leverantor': "Würth Svenska AB",
#       'data': data}

# Use for parsing validation
avsnittNamnSv = {
    "AVSNITT1": "Namnet på ämnet/blandningen och bolaget/företaget",
    "AVSNITT2": "Farliga egenskaper",
    "AVSNITT3": "Sammansättning/information om beståndsdelar",
    "AVSNITT4": "Åtgärder vid första hjälpen",
    "AVSNITT5": "Brandbekämpningsåtgärder",
    "AVSNITT6": "Åtgärder vid oavsiktliga utsläpp",
    "AVSNITT7": "Hantering och lagring",
    "AVSNITT8": "Begränsning av exponeringen/personligt skydd",
    "AVSNITT9": "Fysikaliska och kemiska egenskaper",
    "AVSNITT11": "Toxikologisk information",
    "AVSNITT12": "Ekologisk information",
    "AVSNITT13": "Avfallshantering",
    "AVSNITT14": "Transportinformation",
    "AVSNITT15": "Gällande föreskrifter",
    "AVSNITT16": "Annan information"
}
subAvsnittNamnSv = {
    "1":   "Namnet på ämnet/blandningen och bolaget/företaget",
    "1.1": "Produktbeteckning",
    "1.2": "Relevanta identifierade användningar av ämnet eller blandningen och användningar som det avråds från",
    "1.3": "Närmare upplysningar om den som tillhandahåller säkerhetsdatablad",
    "1.4": "Telefonnummer för nödsituationer",
    "2":   "Farliga egenskaper",
    "2.1": "Klassificering av ämnet eller blandningen",
    "2.2": "Märkningsuppgifter",
    "2.3": "Andra faror",
    "3":   "Sammansättning/information om beståndsdelar",
    "3.2": "Blandningar",
    "4":   "Åtgärder vid första hjälpen",
    "4.1": "Beskrivning av åtgärder vid första hjälpen",
    "4.2": "De viktigaste symptomen och effekterna, både akuta och fördröjda",
    "4.3": "Angivande av omedelbar medicinsk behandling och särskild behandling som eventuellt krävs",
    "5":   "Brandbekämpningsåtgärder",
    "5.1": "Släckmedel",
    "5.2": "Särskilda faror som ämnet eller blandningen kan medföra",
    "5.3": "Råd till brandbekämpningspersonal",
    "6":   "Åtgärder vid oavsiktliga utsläpp",
    "6.1": "Personliga skyddsåtgärder, skyddsutrustning och åtgärder vid nödsituationer",
    "6.2": "Miljöskyddsåtgärder",
    "6.3": "Metoder och material för inneslutning och sanering",
    "6.4": "Hänvisning till andra avsnitt",
    "7":   "Hantering och lagring",
    "7.1": "Skyddsåtgärder för säker hantering",
    "7.2": "Förhållanden för säker lagring, inklusive eventuell oförenlighet",
    "7.3": "Specifik slutanvändning",
    "8":   "Begränsning av exponeringen/personligt skydd",
    "8.1": "Kontrollparametrar",
    "8.2": "Begränsning av exponeringen:",
    "9":   "Fysikaliska och kemiska egenskaper",
    "9.1": "Information om grundläggande fysikaliska och kemiska egenskaper",
    "9.2": "Annan information",
    "10":   "Stabilitet och reaktivitet",
    "10.1": "Reaktivitet",
    "10.2": "Kemisk stabilitet",
    "10.3": "Risken för farliga reaktioner",
    "10.4": "Förhållanden som ska undvikas",
    "10.5": "Oförenliga material",
    "10.6": "Farliga sönderdelningsprodukter",
    "11":   "Toxikologisk information",
    "11.1": "Information om de toxikologiska effekterna",
    "12":   "Ekologisk information",
    "12.1": "Toxicitet",
    "12.2": "Persistens och nedbrytbarhet",
    "12.3": "Bioackumuleringsförmåga",
    "12.4": "Rörligheten i jord",
    "12.5": "Resultat av PBT- och vPvB-bedömningen",
    "12.6": "Andra skadliga effekter",
    "13":   "Avfallshantering",
    "13.1": "Avfallsbehandlingsmetoder",
    "14":   "Transportinformation",
    "14.1": "FN-nummer",
    "14.2": "Officiell transportbenämning",
    "14.3": "Faroklass för transport",
    "14.4": "Förpackningsgrupp",
    "14.5": "Miljöfaror",
    "14.6": "Särskilda försiktighetsåtgärder",
    "14.7": "Bulktransport enligt bilaga II till MARPOL 73/78 och IBC-koden",
    "15":   "Gällande föreskrifter",
    "15.1": "Föreskrifter/lagstiftning om ämnet eller blandningen när det gäller säkerhet, hälsa och miljö",
    "15.2": "Kemikaliesäkerhetsbedömning",
    "16":   "Annan information"
}
