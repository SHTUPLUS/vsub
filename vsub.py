#!/usr/bin/env python
#-------------------
#Author: Shipeng Yan
#-------------------

import subprocess
import argparse
import sys
import os
import random
import time
from datetime import datetime
# Node, queue, gpu, output_file, error_file, name
def parse_args():
    #execute the shell directly

    parser = argparse.ArgumentParser(description="vsub--a tool from venus group that helps you execute command without knowing the pbs usage."
                                                 "The behavior is the totally same as executing shell in your local machine."
                                                 "For example, the tool will execute shell in the current directory in the target node."
                                                 "CAUTION:The default ppn i.e. the allocated number of cpu cores is 1.")


#'-S'
    #parser.add_argument('shell',  help='the shell to execute', nargs='?', type=str, required=True, dest='shell')
    parser.add_argument('program',  help='the program to execute', nargs='?', type=str )
    parser.add_argument('--name','-O',  metavar='XXX', help='The output file name returned by pbs system', type=str, dest='name', default="")
    parser.add_argument('--node', '-N', help='the node you want to run program', type=int, required=True, dest='node')#sist-gpu0x
    parser.add_argument('--dest', '-D', help="the directory of the output file.", type=str, default="./", dest='dest')
    parser.add_argument('--queue', '-Q', metavar='sist-xxx',
                        help='sist-xx. The queue name which could be fould by qstat command(Default:sist-hexm)', type=str,
                        default='sist-hexm', dest='queue')
    parser.add_argument('--stdout', help='Whether to print the output to standard output', type=bool, default=False, dest='stdout_flag')
    parser.add_argument('--shell', action='store_true', help="Is the program just a shell?", dest='shell_flag')
    parser.add_argument('--n_cpu_core', help="The number of cpu cores to be allocated", type=int, default=1, dest='n_cpu_core')
# parser.add_argument('--gpu', '-G', help='specify the gpu', type=list, default=[0,1,2,3], dest='gpu')

    args = parser.parse_args()

    # Convert the input format

    # args.program
    args.program = os.path.expanduser(args.program)

    # args.name
    if args.name is "":
        args.name = os.path.basename(args.program).split('.')[0]

    # args.dest
    args.dest = os.path.expanduser(args.dest)
    # TODO: Check the correctness of arguments
    assert not args.shell_flag or os.path.exists(args.program), "the shell doesn't exist"
    return args


def write_pbs_config_into_shell(file, args):
    """
    parameters
    -----------
    file: the file object returned by open function
    args: parsed by argparser. Including name, queue, node, gpu

    """
    file.write("#PBS -N {}\n".format(args.name))
    file.write("#PBS -q {}\n".format(args.queue))

    node_name = "sist-gpu{:02d}".format(args.node)
    file.write("#PBS -l nodes={}:ppn={}\n".format(node_name, args.n_cpu_core))
    file.write("#PBS -j oe\n")
    cur_path = os.getcwd() #save a bak for recovery

    os.chdir(args.dest)
    dest_dir = os.getcwd()
    out_file = os.path.join(dest_dir, "{}.out.node{:02d}".format(args.name, args.node))
    err_file = os.path.join(dest_dir, "{}.err.node{:02d}".format(args.name, args.node))

    # Overwrite the output file
    open(out_file, 'w').close()
    #open(err_file, 'w').close()

    file.write("#PBS -o {}\n".format(out_file))
    #file.write("#PBS -e {}\n".format(err_file))

    os.chdir(cur_path)
    file.write("cd {}\n".format(cur_path))

    return out_file

def generate_pb_file(args):
    # open file
    os.makedirs(".vsub/", exist_ok=True)
    tmp_file_name = '.vsub/vsub.tmp.' + str(datetime.now()).replace(" ","_")#str(random.randint(0,999))
    tmp_file = open(tmp_file_name,'w')

    if args.shell_flag is False:
        tmp_file.write("#!/bin/bash\n")
        out_file = write_pbs_config_into_shell(tmp_file, args)
        tmp_file.write(args.program + '\n')
    else:
        shell = open(args.program, 'r')
        HAVE_NON_BLANK_LINE = False
        FIRST_LINE_PATTERNS = ['/usr/bin/env', 'python', 'bash']
        for i, line in enumerate(shell):
            line = line.strip()
            if len(line)==0:
                continue # Remove blank lines
            if not HAVE_NON_BLANK_LINE:
                if line[0]!='#' or 'PBS ' in line:
                    raise ValueError("First line should start with # and specify which interpreter you want to use")
                if not any([patt in line for patt in FIRST_LINE_PATTERNS]):
                    raise Warning("There maybe something wrong in the first line.")
                tmp_file.write(line+'\n')
                out_file = write_pbs_config_into_shell(tmp_file, args)
                HAVE_NON_BLANK_LINE = True
            elif 'PBS' in line and '#' in line:
                continue
            else:
                tmp_file.write(line+'\n')

        shell.close()

    tmp_file.close()

    return tmp_file_name, out_file


def execute_pb_file(tmpshell_name, out_file, stdout_flag):
    """
    :parameter
    ------------
    file: file name
    """
    cmd = ['qsub', tmpshell_name]
    output = subprocess.run(cmd, stdout=subprocess.PIPE)

    task_id = output.stdout.decode('utf-8').strip().split('.')[0]
    print("task_id: ", task_id)
    print("out_file: ", out_file)
    print("less -f {}".format(out_file))
    #TODO: Spawn a new process to watch the task_no status, and stop the less command when process exit
    if stdout_flag:
        try:
            while not os.path.exists(out_file):
                time.sleep(0.5)
            subprocess.call(['less', '-f', out_file])
        except KeyboardInterrupt:
            # subprocess.call(['rm', tmpshell_name])
            return
    # TODO: Now, we can't remove the shell for the correctness.
    # subprocess.call(['rm', tmpshell_name])


if __name__ == '__main__':
    args = parse_args()
    pb_file_name, out_file = generate_pb_file(args)
    execute_pb_file(pb_file_name, out_file, args.stdout_flag)



# TODO: stdout_flag









