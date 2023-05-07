import sys
import csv
import datetime
import asana
import json
import pandas as pd
import threading
import time

#Personal Information
MY_PERSONAL_ACCESS_TOKEN = '[MY_ACCESS_TOKEN]'
my_workspace = '[MY_WORKSPACE]'

#Configs
OUT_FILE_PATH = r'C:\Export_Asana'
OUT_FILE_NAME_ROOT = 'asana_tasks'
CSV_OUT_HEADER = ['proj_name', 'ms_name', 'task_title', 'completed_on', 'priority']
REC_LIMIT = 99999
ITEM_LIMIT = 100
PAGE_SIZE = 50

#Output File Columns
CSV_OUT_HEADER = ['Task', 'Project', 'Workspace', 'DueDate', 'CreatedAt', \
                  'ModifiedAt', 'Completed', 'CompletedAt', 'Assignee', 'AssigneeStatus', \
                  'Parent', 'Notes', 'TaskId', 
                  'SubTask', 'SubDueDate', 'SubCreatedAt', \
                  'SubModifiedAt', 'SubCompleted', 'SubCompletedAt', 'SubAssignee', 'SubAssigneeStatus', \
                  'SubNotes', 'SubTaskId', 'Tags', 'SubTags', 'Section','EstimatedHours','ActualHours', \
                  'SubEstimatedHours','SubActualHours','Team','Gerenciadora','PontuacaoPrioridade','LinkTrello', \
                  'Departamento','Status','TipoContrato','SquadTI','TipoSistema','TipoSolicitacao','Prioridade','Solicitante', \
                  'TipoCard','StatusBI','TipoDemandaBI','IdLicitacao'
                 ]

#Return a subtask with columns bellow.
def subtasks(id,client, task):
    smart_id = [id]
    subtasks = []
    for idx in smart_id:
        time.sleep(0.15)
        task_subtasks = client.tasks.subtasks(idx, {"opt_fields":"name, \
                        projects, workspace, gid, due_on, created_at, modified_at, completed, \
                        completed_at, assignee.name, assignee_status, parent, notes, tags.name, \
                        custom_fields.name, custom_fields.number_value,num_subtasks"})
        for task in task_subtasks:
            subtasks.append(task)
            if task['num_subtasks'] > 0:
                smart_id.append(task['gid']) 
                           
    if len(subtasks) == 0:
        subtasks = task
    return(subtasks)

#Write out selected columns into CSV file.
def write_csv_records(csv_out_file_name, csv_header_list, csv_record_list):
    with open(csv_out_file_name, 'w', encoding='utf8') as csv_file:
        csvwriter = csv.writer(csv_file, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        csvwriter.writerow(csv_header_list)
        for item in csv_record_list:
            csvwriter.writerow(item)
    return

#Return a dictionary with id and name of each workspace.
def get_workspace_dict(workspaces):
    this_dict = {}
    for workspace in workspaces:
        this_dict[workspace['gid']] = workspace['name']
    return this_dict

#Function used to looping all projects in parallel
def process_project_tasks(client, project, ws_dict, csv_out_file):
    """Add each task for the current project to the records list."""
    task_list = []
    tasks = []
    
    #Save all projects information into tasklist
    while True:
        tasks_now = client.tasks.find_by_project(project['gid'], {"opt_fields":"name, \
            projects, workspace, gid, due_on, created_at, modified_at, completed, \
            completed_at, assignee.name, assignee_status, parent, notes, tags.name, memberships.section.name, \
            custom_fields.name, custom_fields.number_value, custom_fields.text_value, custom_fields.display_value"})
        
        for task in tasks_now:
            tasks.append(task)
              
        if 'next_page' not in tasks:
            break
    
    #Looping all tasks in Projects -> Tasks        
    for task in tasks:
        #Custom Fields
        EstimatedHours = 0
        ActualHours = 0
        Gerenciadora = ''
        PontuacaoPrioridade = 0
        LinkTrello = ''
        Departamento = ''
        Status = ''
        TipoContrato = ''
        SquadTI = ''
        TipoSistema = ''
        TipoSolicitacao = ''
        Prioridade = ''
        Solicitante = ''
        TipoCard = ''
        StatusBI = ''
        TipoDemandaBI = ''
        IdLicitacao = 0
         
        #Get information from custom fields
        for ctm in task['custom_fields']:
            if ctm['name'] == 'Estimated hours':
                EstimatedHours = ctm.get('number_value') or ''
            if ctm['name'] == 'Actual hours':
                ActualHours = ctm.get('number_value') or ''
            if ctm['name'] == 'Gerenciadora':
                Gerenciadora = ctm.get('display_value') or ''
            if ctm['name'] == 'Pontuação Prioridade':
                PontuacaoPrioridade = ctm.get('text_value') or ''
            if ctm['name'] == 'P - Link Trello':
                LinkTrello = ctm.get('display_value') or ''
            if ctm['name'] == 'Departamentos':
                Departamento = ctm.get('display_value') or ''
            if ctm['name'] == 'Status':
                Status = ctm.get('display_value') or ''
            if ctm['name'] == 'Tipo do contrato':
                TipoContrato = ctm.get('display_value') or ''
            if ctm['name'] == 'Squad TI':
                SquadTI = ctm.get('display_value') or ''
            if ctm['name'] == 'Tipo Sistema':
                TipoSistema = ctm.get('display_value') or ''
            if ctm['name'] == 'Tipo de Solicitação':
                TipoSolicitacao = ctm.get('display_value') or ''
            if ctm['name'] == 'Prioridade':
                Prioridade = ctm.get('display_value') or ''
            if ctm['name'] == 'Solicitante':
                Solicitante = ctm.get('display_value') or ''
            if ctm['name'] == 'Tipo do Card' or ctm['name'] == 'Tipo de Card':
                TipoCard = ctm.get('display_value') or ''
            if ctm['name'] == 'Status BI':
                StatusBI = ctm.get('display_value') or '' 
            if ctm['name'] == 'Tipo Demanda BI':
                TipoDemandaBI = ctm.get('display_value') or ''
            if ctm['name'] == 'ID Licitação':
                IdLicitacao = ctm.get('display_value') or '' 
               
                
        #Get section name where task are
        for idxsection in task['memberships']:
            if idxsection['section']['gid'] in list(pd.DataFrame(project['sections'])['gid']):
                ws_name = ws_dict[task['workspace']['gid']]
                
                # Truncate most of long notes -- I don't need the details
                if len(task['notes']) > 80:
                    task['notes'] = task['notes'][:79]
                    
                #Get fields from a task
                assignee = task['assignee']['name'] if task['assignee'] is not None else ''
                created_at = task['created_at'][0:10] + ' ' + task['created_at'][11:16] if \
                        task['created_at'] is not None else None
                modified_at = task['modified_at'][0:10] + ' ' + task['modified_at'][11:16] if \
                        task['modified_at'] is not None else None
                completed_at = task['completed_at'][0:10] + ' ' + task['completed_at'][11:16] if \
                    task['completed_at'] is not None else None
                
                #Concat all tag in string
                tag = ''
                for idx in task['tags']:
                    tag = tag + idx['name'] + ', '

                smart_id = task['gid'] 
                if True: #used for testing
                    task_subtasks = subtasks(smart_id, client, [task])
                    #print(task_subtasks) #used for testing
                    
                    for subtask in task_subtasks:
                        #Get estimated hours
                        SubEstimatedHours = 0
                        SubActualHours = 0
                        for Subctm in subtask['custom_fields']:
                            if Subctm['name'] == 'Estimated hours':
                                SubEstimatedHours = Subctm['number_value']
                            if Subctm['name'] == 'Actual hours':
                                SubActualHours = Subctm['number_value']
                        
                        #Get assigne and tag from subtasks
                        subassignee = subtask['assignee']['name'] if subtask['assignee'] is not None else ''
                        subtag = ''
                        for idx in subtask['tags']:
                            subtag = subtag + idx['name'] +', '
                        
                        #check if exists substask from a task to save csv
                        if len(subtask) > 0:
                            rec = [task['name'], project['name'], ws_name, task['due_on'], created_at, \
                                    modified_at, task['completed'], completed_at, assignee, \
                                    task['assignee_status'], task['parent'], task['notes'], task['gid'], \

                                    subtask['name'], subtask['due_on'], subtask['created_at'], \
                                    subtask['modified_at'], subtask['completed'], subtask['completed_at'], \
                                    subassignee, subtask['assignee_status'], subtask['notes'], subtask['gid'], \
                                    tag,subtag,idxsection['section']['name'],EstimatedHours,ActualHours, \
                                    SubEstimatedHours,SubActualHours,project['team']['name'], \
                                    Gerenciadora,PontuacaoPrioridade,LinkTrello,Departamento,Status,TipoContrato,SquadTI, \
                                    TipoSistema,TipoSolicitacao,Prioridade,Solicitante,TipoCard,StatusBI,TipoDemandaBI,IdLicitacao
                                    ]
                            
                            #append into a list
                            rec = ['' if s is None else s for s in rec]
                            task_list.append(rec)
                            
                        else:
                            #Haven't subtasks from a task 
                            rec = [task['name'], project['name'], ws_name, task['due_on'], created_at, \
                                    modified_at, task['completed'], completed_at, assignee, \
                                    task['assignee_status'], task['parent'], task['notes'], task['gid'], \

                                    '', '', '', \
                                    '', '', '', \
                                    '', '', '', '', \
                                    tag,'',idxsection['section']['name'],EstimatedHours,ActualHours, \
                                    SubEstimatedHours,SubActualHours,project['team']['name'], \
                                    Gerenciadora,PontuacaoPrioridade,LinkTrello,Departamento,Status,TipoContrato,SquadTI, \
                                    TipoSistema,TipoSolicitacao,Prioridade,Solicitante,TipoCard,StatusBI,TipoDemandaBI,IdLicitacao
                                    ]
                            
                            #append into a list
                            rec = ['' if s is None else s for s in rec]
                            task_list.append(rec)
                            
                        if task_subtasks == task:
                            break
    #print csv name    
    print('arquivo ',csv_out_file)
    
    #save csv into diretory
    write_csv_records(csv_out_file, CSV_OUT_HEADER, task_list)
    return 

def main():
    """Main program loop."""
    def usage():
        """Show usage if user does not supply parameters."""
        text = """usage: asana2csv.py 'fitcard.com.br'"""

        print(text)
    
    #connect from asana
    client = asana.Client.access_token(MY_PERSONAL_ACCESS_TOKEN)
    client.options['page_size'] = 100
    client.headers={'asana-enable': 'string_ids'}
    my_client = client.users.me()
    
    #get workspace
    ws_dict = get_workspace_dict(my_client['workspaces'])
    
    #get all projects from a workspace
    this_workspace = next(workspace for workspace in my_client['workspaces'] if \
        workspace['name'] == my_workspace)
    all_projects = client.projects.find_by_workspace(this_workspace['gid'], {"opt_fields":"sections.gid, sections.name, name, team.name"})

    #looping all porjects in parallel
    for project in all_projects:
        if True: #project['gid'] == '1185351678792284': #used for testing
            my_filename = '_'.join([OUT_FILE_NAME_ROOT, my_workspace + str(project['gid']) + '.csv'])
            csv_out_file = '/'.join([OUT_FILE_PATH, my_filename])
            t = threading.Thread(target=process_project_tasks, args=(client, project, ws_dict, csv_out_file,))
            t.name = project['gid']
            t.start()
    return

#call main function
main()
