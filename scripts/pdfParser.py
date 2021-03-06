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
#       'leverantor': "W??rth Svenska AB",
#       'data': data}

# Use for parsing validation
avsnittNamnSv = {
    "AVSNITT1": "Namnet p?? ??mnet/blandningen och bolaget/f??retaget",
    "AVSNITT2": "Farliga egenskaper",
    "AVSNITT3": "Sammans??ttning/information om best??ndsdelar",
    "AVSNITT4": "??tg??rder vid f??rsta hj??lpen",
    "AVSNITT5": "Brandbek??mpnings??tg??rder",
    "AVSNITT6": "??tg??rder vid oavsiktliga utsl??pp",
    "AVSNITT7": "Hantering och lagring",
    "AVSNITT8": "Begr??nsning av exponeringen/personligt skydd",
    "AVSNITT9": "Fysikaliska och kemiska egenskaper",
    "AVSNITT11": "Toxikologisk information",
    "AVSNITT12": "Ekologisk information",
    "AVSNITT13": "Avfallshantering",
    "AVSNITT14": "Transportinformation",
    "AVSNITT15": "G??llande f??reskrifter",
    "AVSNITT16": "Annan information"
}
subAvsnittNamnSv = {
    "1":   "Namnet p?? ??mnet/blandningen och bolaget/f??retaget",
    "1.1": "Produktbeteckning",
    "1.2": "Relevanta identifierade anv??ndningar av ??mnet eller blandningen och anv??ndningar som det avr??ds fr??n",
    "1.3": "N??rmare upplysningar om den som tillhandah??ller s??kerhetsdatablad",
    "1.4": "Telefonnummer f??r n??dsituationer",
    "2":   "Farliga egenskaper",
    "2.1": "Klassificering av ??mnet eller blandningen",
    "2.2": "M??rkningsuppgifter",
    "2.3": "Andra faror",
    "3":   "Sammans??ttning/information om best??ndsdelar",
    "3.2": "Blandningar",
    "4":   "??tg??rder vid f??rsta hj??lpen",
    "4.1": "Beskrivning av ??tg??rder vid f??rsta hj??lpen",
    "4.2": "De viktigaste symptomen och effekterna, b??de akuta och f??rdr??jda",
    "4.3": "Angivande av omedelbar medicinsk behandling och s??rskild behandling som eventuellt kr??vs",
    "5":   "Brandbek??mpnings??tg??rder",
    "5.1": "Sl??ckmedel",
    "5.2": "S??rskilda faror som ??mnet eller blandningen kan medf??ra",
    "5.3": "R??d till brandbek??mpningspersonal",
    "6":   "??tg??rder vid oavsiktliga utsl??pp",
    "6.1": "Personliga skydds??tg??rder, skyddsutrustning och ??tg??rder vid n??dsituationer",
    "6.2": "Milj??skydds??tg??rder",
    "6.3": "Metoder och material f??r inneslutning och sanering",
    "6.4": "H??nvisning till andra avsnitt",
    "7":   "Hantering och lagring",
    "7.1": "Skydds??tg??rder f??r s??ker hantering",
    "7.2": "F??rh??llanden f??r s??ker lagring, inklusive eventuell of??renlighet",
    "7.3": "Specifik slutanv??ndning",
    "8":   "Begr??nsning av exponeringen/personligt skydd",
    "8.1": "Kontrollparametrar",
    "8.2": "Begr??nsning av exponeringen:",
    "9":   "Fysikaliska och kemiska egenskaper",
    "9.1": "Information om grundl??ggande fysikaliska och kemiska egenskaper",
    "9.2": "Annan information",
    "10":   "Stabilitet och reaktivitet",
    "10.1": "Reaktivitet",
    "10.2": "Kemisk stabilitet",
    "10.3": "Risken f??r farliga reaktioner",
    "10.4": "F??rh??llanden som ska undvikas",
    "10.5": "Of??renliga material",
    "10.6": "Farliga s??nderdelningsprodukter",
    "11":   "Toxikologisk information",
    "11.1": "Information om de toxikologiska effekterna",
    "12":   "Ekologisk information",
    "12.1": "Toxicitet",
    "12.2": "Persistens och nedbrytbarhet",
    "12.3": "Bioackumuleringsf??rm??ga",
    "12.4": "R??rligheten i jord",
    "12.5": "Resultat av PBT- och vPvB-bed??mningen",
    "12.6": "Andra skadliga effekter",
    "13":   "Avfallshantering",
    "13.1": "Avfallsbehandlingsmetoder",
    "14":   "Transportinformation",
    "14.1": "FN-nummer",
    "14.2": "Officiell transportben??mning",
    "14.3": "Faroklass f??r transport",
    "14.4": "F??rpackningsgrupp",
    "14.5": "Milj??faror",
    "14.6": "S??rskilda f??rsiktighets??tg??rder",
    "14.7": "Bulktransport enligt bilaga II till MARPOL 73/78 och IBC-koden",
    "15":   "G??llande f??reskrifter",
    "15.1": "F??reskrifter/lagstiftning om ??mnet eller blandningen n??r det g??ller s??kerhet, h??lsa och milj??",
    "15.2": "Kemikalies??kerhetsbed??mning",
    "16":   "Annan information"
}
