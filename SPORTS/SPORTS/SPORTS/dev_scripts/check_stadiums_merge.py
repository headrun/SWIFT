import json

json_data= open('/home/jbethala/DATA_WIKIPEDIA_ENTERTAINMENT_MC_FILE.json').read()
seed_dict = json.loads(json_data)

lines = open('stadiums_check.txt').readlines()
del_file = open('std_out/stadiums_merge_delete.txt', 'w')
same_type = open('std_out/stadiums_merge_review.txt', 'w')
vt_no_file = open('std_out/stadiums_merge_no_vt.txt', 'w')
seed_no = open('std_out/stadiums_merge_missing_from_seed.txt', 'w')

for line in lines:
    line = line.replace('In Old not in New:', '').strip('\n ')
    wiki_gid = line.split()[0].strip()
    child_gid = line.split()[1].strip()
    try:
        wiki_values = seed_dict[wiki_gid]
        try:
            _type = wiki_values['Vt']
            if _type != 'stadium':
                del_file.write('%s<>%s<>%s\n'%(wiki_gid, child_gid, _type))
            else:
                same_type.write('%s<>%s<>%s\n'%(wiki_gid, child_gid, _type))
        except KeyError, e:
            vt_no_file.write('%s<>%s\n'%(wiki_gid, child_gid))
    except KeyError, e:
        seed_no.write('%s<>%s\n'%(wiki_gid, child_gid))
