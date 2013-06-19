/** Copyright 2013 BlackBerry, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. 
 */

package com.rim.logdriver.admin;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HFind extends Configured implements Tool {
  private Deque<Path> paths = new ArrayDeque<Path>();
  private List<FileStatusFilter> tests = new ArrayList<FileStatusFilter>();
  private List<FileStatusFilter> actions = new ArrayList<FileStatusFilter>();

  private Map<Path, FileStatus> fileStatusCache = new HashMap<Path, FileStatus>();

  @Override
  public int run(String[] args) throws Exception {
    final long startTime = System.currentTimeMillis();

    int i = 0;
    while (i < args.length) {
      if (args[i].startsWith("-")) {
        break;
      }

      Path path = new Path(args[i]);
      FileSystem fs = path.getFileSystem(getConf());
      FileStatus[] fileStatuses = fs.globStatus(path);
      if (fileStatuses != null) {
        for (FileStatus fileStatus : fileStatuses) {
          paths.add(fileStatus.getPath());
          fileStatusCache.put(fileStatus.getPath(), fileStatus);
        }
      }

      i++;
    }

    while (i < args.length) {
      // -print action
      if ("-print".equals(args[i])) {
        actions.add(new FileStatusFilter() {
          @Override
          public boolean accept(FileStatus fileStatus) {
            System.out.println(fileStatus.getPath());
            return true;
          }
        });
      }

      // -delete action
      if ("-delete".equals(args[i])) {
        actions.add(new FileStatusFilter() {
          @Override
          public boolean accept(FileStatus fileStatus) {
            try {
              FileSystem fs = fileStatus.getPath().getFileSystem(getConf());
              if (!fileStatus.isDir()
                  || fs.listStatus(fileStatus.getPath()).length == 0) {
                return fs.delete(fileStatus.getPath(), true);
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
            return false;
          }
        });
      }

      // -atime test
      else if ("-atime".equals(args[i])) {
        i++;
        if (i >= args.length) {
          System.err.println("Missing arguement for -atime");
          System.exit(1);
        }

        String t = args[i];
        if (t.charAt(0) == '+') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getAccessTime())
                  / (24 * 60 * 60 * 1000) > time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else if (t.charAt(0) == '-') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getAccessTime())
                  / (24 * 60 * 60 * 1000) < time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else {
          final long time = Long.parseLong(t);
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getAccessTime())
                  / (24 * 60 * 60 * 1000) == time) {
                return true;
              } else {
                return false;
              }
            }
          });
        }
      }

      // -mtime test
      else if ("-mtime".equals(args[i])) {
        i++;
        if (i >= args.length) {
          System.err.println("Missing arguement for -mtime");
          System.exit(1);
        }

        String t = args[i];
        if (t.charAt(0) == '+') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getModificationTime())
                  / (24 * 60 * 60 * 1000) > time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else if (t.charAt(0) == '-') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getModificationTime())
                  / (24 * 60 * 60 * 1000) < time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else {
          final long time = Long.parseLong(t);
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getModificationTime())
                  / (24 * 60 * 60 * 1000) == time) {
                return true;
              } else {
                return false;
              }
            }
          });
        }
      }

      // -amin test
      else if ("-amin".equals(args[i])) {
        i++;
        if (i >= args.length) {
          System.err.println("Missing arguement for -amin");
          System.exit(1);
        }

        String t = args[i];
        if (t.charAt(0) == '+') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getAccessTime()) / (60 * 1000) > time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else if (t.charAt(0) == '-') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getAccessTime()) / (60 * 1000) < time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else {
          final long time = Long.parseLong(t);
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getAccessTime()) / (60 * 1000) == time) {
                return true;
              } else {
                return false;
              }
            }
          });
        }
      }

      // -mmin test
      else if ("-mmin".equals(args[i])) {
        i++;
        if (i >= args.length) {
          System.err.println("Missing arguement for -mmin");
          System.exit(1);
        }

        String t = args[i];
        if (t.charAt(0) == '+') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getModificationTime()) / (60 * 1000) > time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else if (t.charAt(0) == '-') {
          final long time = Long.parseLong(t.substring(1));
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getModificationTime()) / (60 * 1000) < time) {
                return true;
              } else {
                return false;
              }
            }
          });
        } else {
          final long time = Long.parseLong(t);
          tests.add(new FileStatusFilter() {
            @Override
            public boolean accept(FileStatus fileStatus) {
              if ((startTime - fileStatus.getModificationTime()) / (60 * 1000) == time) {
                return true;
              } else {
                return false;
              }
            }
          });
        }
      }

      // -regex test
      else if ("-regex".equals(args[i])) {
        i++;
        if (i >= args.length) {
          System.err.println("Missing arguement for -regex");
          System.exit(1);
        }

        final Pattern p = Pattern.compile(args[i]);
        tests.add(new FileStatusFilter() {
          @Override
          public boolean accept(FileStatus fileStatus) {
            if (p.matcher(fileStatus.getPath().toString()).matches()) {
              return true;
            } else {
              return false;
            }
          }
        });
      }

      i++;
    }

    if (actions.size() == 0) {
      actions.add(new FileStatusFilter() {
        @Override
        public boolean accept(FileStatus fileStatus) {
          System.out.println(fileStatus.getPath());
          return true;
        }
      });
    }

    search();

    return 0;
  }

  private void search() throws IOException {
    Set<Path> seen = new HashSet<Path>();
    while (paths.size() > 0) {

      // Check if the top of the list has any children. If so, add them to the
      // stack. If not, then process it.
      Path p = paths.peekFirst();
      FileSystem fs = p.getFileSystem(getConf());

      FileStatus fileStatus = fileStatusCache.get(p);
      // Only check if we haven't seen this before.
      if (fileStatus.isDir() && seen.contains(p) == false) {
        FileStatus[] fileStatuses = fs.listStatus(p);
        if (fileStatuses != null && fileStatuses.length > 0) {
          for (FileStatus x : fileStatuses) {
            paths.addFirst(x.getPath());
            fileStatusCache.put(x.getPath(), x);
          }
          seen.add(p);
          continue;
        }
      }

      // If we get here, then we should be processing the path.
      p = paths.removeFirst();
      // If we're processing it, we won't need it's status in the cache anymore.
      fileStatusCache.remove(p);

      boolean match = true;
      for (FileStatusFilter test : tests) {
        try {
          if (test.accept(fileStatus) == false) {
            match = false;
            break;
          }
        } catch (Throwable t) {
          t.printStackTrace();
          System.err.println("path=" + p + " fileStatus=" + fileStatus);
        }
      }
      if (match == false) {
        continue;
      }

      for (FileStatusFilter action : actions) {
        try {
          if (action.accept(fileStatus) == false) {
            match = false;
            break;
          }
        } catch (Throwable t) {
          t.printStackTrace();
          System.err.println("path=" + p + " fileStatus=" + fileStatus);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {

    int res = ToolRunner.run(new Configuration(), new HFind(), args);
    System.exit(res);
  }

  private abstract class FileStatusFilter {
    abstract public boolean accept(FileStatus fileStatus);
  }
}
