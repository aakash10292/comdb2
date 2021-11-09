/*
   Copyright 2017 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef INCLUDED_MESSAGE_QUEUE_H
#define INCLUDED_MESSAGE_QUEUE_H

struct comdb2_queue_publisher {
    int type;
    int (*publish)(void *buf, int buf_len);
};

struct comdb2_queue_subscriber {
    int type;
    void (*subscribe)();
};
typedef struct comdb2_queue_publisher comdb2_queue_pub_t;
typedef struct comdb2_queue_subscriber comdb2_queue_sub_t;
extern comdb2_queue_pub_t *log_publisher_plugin;
#endif /* !__INCLUDED_MESSAGE_QUEUE_H */
