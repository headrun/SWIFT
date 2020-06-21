from MySQLdb import escape_string
from Yocket.items import CSVItem

class YocketPipeline():

    def get_source(self, spider):
        return spider.name.split('_', 1)[0].strip()

    def process_item(self, item, spider):
        if isinstance(item, CSVItem):
            csv_values = (
                item["ProfileName"], item["UndergradDegree"], item["UndergradUniversity"], item["UndergradCgpa"],
                item["Experience"], item["WorkExperience"], item["CompanyName"], item["Jobtitle"], item["Techpapers"],
                item["Numberofresearch"], item["Skills"], item["InterestedTermandYear"], item["InterestedCourse"],
                item["GRETotalScore"], item["GREVerbalScore"], item["GREQuantScore"], item["TOEFLSCORE"], item["IELTSSCORE"],
                item["Applieduniversity"], item["Appliedcourse"], item["Applieddate"], item["Status"], item["Link"],
                item["SourceUniversity"]
            )

            spider.csv_writer.writerow(csv_values)

        return item
