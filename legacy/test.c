
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <linux/can.h>
#include <stdlib.h>
#include <unistd.h>
#include <linux/can/raw.h>
#include <time.h>
#include <stdint.h>

int soc;
int read_can_port;

int open_port(const char *port)
{
    struct ifreq ifr;
    struct sockaddr_can addr;
    soc = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if(soc < 0)
        return (-1);
    addr.can_family = AF_CAN;
    strcpy(ifr.ifr_name, port);
    if (ioctl(soc, SIOCGIFINDEX, &ifr) < 0)
        return (-1);
    addr.can_ifindex = ifr.ifr_ifindex;
    fcntl(soc, F_SETFL, O_NONBLOCK);
    if (bind(soc, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        return (-1);
    return 0;
}

int send_port(struct can_frame *frame)
{
    const int retval = write(soc, frame, sizeof(struct can_frame));
    if (retval != sizeof(struct can_frame))
        return (-1);
    return (0);
}

void read_port()
{
    read_can_port = 1;
    while(read_can_port)
    {
        struct timeval timeout = {1, 0};
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(soc, &readSet);
        if (select((soc + 1), &readSet, NULL, NULL, &timeout) >= 0)
        {
            if (read_can_port && FD_ISSET(soc, &readSet))
            {
    		struct can_frame frame_rd;
                const int recvbytes = read(soc, &frame_rd, sizeof(struct can_frame));
                if(recvbytes)
                {
		    time_t timestamp_sec;
		    time(&timestamp_sec);
                    char xf[255]="";
		    for (int a=0; a<frame_rd.can_dlc; a++) {
			char tm[4];
			sprintf(tm, "%02X", frame_rd.data[a]);
			strcat(xf, tm);
		    }
		    for (int b=0; b < (8 - frame_rd.can_dlc); b++)
			strcat(xf, "  ");
                    fprintf(stdout, "%d,%x,%d,%s", timestamp_sec, frame_rd.can_id, frame_rd.can_dlc, xf);

uint8_t program_type = frame_rd.can_id & 0xFF;
uint8_t subscriber_id = (frame_rd.can_id >> 8) & 0xFF;
uint8_t function_type = (frame_rd.can_id >> 16) & 0xFF;
uint8_t protocol_type = (frame_rd.can_id >> 24) & 0x7;
uint8_t can_message_type = (frame_rd.can_id >> 27) & 0x3;
fprintf(stdout, " ... ProgramType=0x%02X SubscriberID=0x%02X FunctionType=0x%02X ProtocolType=%d MessageType=%d", program_type, subscriber_id, function_type, protocol_type, can_message_type);

const char* msg_type_str = "UNKNOWN";
switch (can_message_type) {
    case 0: msg_type_str = "MSG_REQUEST"; break;
    case 1: msg_type_str = "MSG_RESERVE"; break;
    case 2: msg_type_str = "MSG_RESPONSE"; break;
    case 3: msg_type_str = "MSG_ERROR"; break;
}



if (program_type == 0x0B) {
    const char* func_type_str = "UNKNOWN";
    switch (function_type) {
        case 0: func_type_str = "HAS_ANYBODY_HERE"; break;
        case 1: func_type_str = "I_AM_HERE"; break;
        case 2: func_type_str = "GET_CONTROLLER_ID"; break;
        case 3: func_type_str = "GET_ACTIVE_PROGRAMS_LIST"; break;
        case 4: func_type_str = "ADD_PROGRAM"; break;
        case 5: func_type_str = "REMOVE_PROGRAM"; break;
        case 6: func_type_str = "GET_SYSTEM_DATE_TIME"; break;
        case 7: func_type_str = "SET_SYSTEM_DATE_TIME"; break;
        case 8: func_type_str = "I_AM_RESETED"; break;
        case 9: func_type_str = "DATALOGGER_TEST"; break;
    }
    fprintf(stdout, " ... CONTROLLER (0x0B): Function=0x%02X (%s), MessageType=0x%X (%s), SubscriberID=0x%02X", function_type, func_type_str, can_message_type, msg_type_str, subscriber_id);
    if (frame_rd.can_dlc > 0) {
        if ((function_type == 0x01 || function_type == 0x08) && frame_rd.can_dlc >= 4) {
            uint8_t device_subscriber_id = frame_rd.data[0];
            uint8_t device_id = frame_rd.data[1];
            uint8_t oem_id = frame_rd.data[2];
            uint8_t device_variant = frame_rd.data[3];
            fprintf(stdout, " ... Payload: SubscriberID=0x%02X, DeviceID=0x%02X, OEM_ID=0x%02X, DeviceVariant=0x%02X\n", device_subscriber_id, device_id, oem_id, device_variant);
        } else {
            fprintf(stdout, " ... Payload: ");
            for (int i = 0; i < frame_rd.can_dlc; i++)
                fprintf(stdout, "0x%02X ", frame_rd.data[i]);
            fprintf(stdout, "\n");
        }
    }
}



else if (program_type == 0x90) {
    const char* func_type_str = "UNKNOWN";
    switch (function_type) {
        case 0: func_type_str = "epsc_connect"; break;
        case 1: func_type_str = "epsc_factoryReset"; break;
    }
    fprintf(stdout, " ... PARAMETERSYNCCONFIG (0x90): Function=0x%02X (%s), MessageType=0x%X (%s), SubscriberID=0x%02X", function_type, func_type_str, can_message_type, msg_type_str, subscriber_id);
    if (frame_rd.can_dlc > 0) {
        if (function_type == 0x00 && frame_rd.can_dlc >= 2) {
            uint8_t source = frame_rd.data[0];
            uint8_t destination = frame_rd.data[1];
            fprintf(stdout, " ... Payload: Source=0x%02X, Destination=0x%02X\n", source, destination);
        } else if (function_type == 0x01 && frame_rd.can_dlc >= 6) {
            uint8_t source = frame_rd.data[0];
            uint8_t destination = frame_rd.data[1];
            uint8_t family = frame_rd.data[2];
            uint8_t synchronize = frame_rd.data[3];
            uint8_t disconnect = frame_rd.data[4];
            uint8_t selftestdone = frame_rd.data[5];
            fprintf(stdout, " ... Payload: Source=0x%02X, Destination=0x%02X, Family=0x%02X, Synchronize=0x%02X, Disconnect=0x%02X, SelfTestDone=0x%02X\n", source, destination, family, synchronize, disconnect, selftestdone);
        } else {
            fprintf(stdout, " ... Payload: ");
            for (int i = 0; i < frame_rd.can_dlc; i++)
                fprintf(stdout, "0x%02X ", frame_rd.data[i]);
            fprintf(stdout, "\n");
        }
    }
}



else if (program_type == 0x83) {
    const char* func_type_str = "UNKNOWN";
    switch (function_type) {
        case 0x00: func_type_str = "ecsOutdor"; break;
        case 0x01: func_type_str = "ecsFlow"; break;
        case 0x02: func_type_str = "ecsCold"; break;
        case 0x03: func_type_str = "ecsSolar"; break;
        case 0x04: func_type_str = "ecsReserved1"; break;
        case 0x05: func_type_str = "ecsReserved2"; break;
        case 0x06: func_type_str = "ecsReserved3"; break;
        case 0x07: func_type_str = "ecsStorage"; break;
        case 0x08: func_type_str = "ecsStorageTop"; break;
        case 0x09: func_type_str = "ecsStorageCenter"; break;
        case 0x0A: func_type_str = "ecsStorageBottom"; break;
        case 0x0B: func_type_str = "ecsReserved4"; break;
        case 0x0C: func_type_str = "ecsRcTset"; break;
        case 0x0D: func_type_str = "ecsRcHumidity"; break;
        case 0x0E: func_type_str = "ecsRcTroom"; break;
        case 0x0F: func_type_str = "ecsRcWheel"; break;
        case 0x10: func_type_str = "ecsRcSwitch"; break;
        case 0x16: func_type_str = "ecsRcTflow"; break;
    }
    fprintf(stdout, " ... REMOTESENSOR (0x%02X): Function=0x%02X (%s), MessageType=0x%X (%s), SubscriberID=0x%02X", program_type, function_type, func_type_str, can_message_type, msg_type_str, subscriber_id);
    if (can_message_type == 0)
        fprintf(stdout, " ... Request: No payload expected\n");
    else if (can_message_type == 2) {
        if (frame_rd.can_dlc == 4) {
            uint16_t value = (frame_rd.data[1] << 8) | frame_rd.data[0];
            uint8_t virtual_circuit = frame_rd.data[2];
            uint8_t controller_type = frame_rd.data[3];
            fprintf(stdout, " ... Response: Value=0x%04X (%d), VirtualCircuit=0x%02X, ControllerType=0x%02X\n", value, value, virtual_circuit, controller_type);
        } else if (frame_rd.can_dlc == 6) {
            uint16_t value = (frame_rd.data[1] << 8) | frame_rd.data[0];
            uint8_t virtual_circuit = frame_rd.data[2];
            uint8_t controller_type = frame_rd.data[3];
            uint8_t room_index = frame_rd.data[4];
            uint8_t room_type = frame_rd.data[5];
            fprintf(stdout, " ... CALEON Response: Value=0x%04X (%d), VirtualCircuit=0x%02X, ControllerType=0x%02X, RoomIndex=0x%02X, RoomType=0x%02X\n", value, value, virtual_circuit, controller_type, room_index, room_type);
        } else {
            fprintf(stdout, " ... Payload (unknown format): ");
            for (int i = 0; i < frame_rd.can_dlc; i++)
                fprintf(stdout, "0x%02X ", frame_rd.data[i]);
            fprintf(stdout, "\n");
        }
    } else {
        fprintf(stdout, " ... Payload: ");
        for (int i = 0; i < frame_rd.can_dlc; i++)
            fprintf(stdout, "0x%02X ", frame_rd.data[i]);
        fprintf(stdout, "\n");
    }
}



else if (program_type == 0x84) {
    fprintf(stdout, " ... NAMEDSENSOR (0x%02X): Function=0x%02X, MessageType=0x%X (%s), SubscriberID=0x%02X", program_type, function_type, can_message_type, msg_type_str, subscriber_id);
    if (can_message_type == 0)
        fprintf(stdout, " ... Request: No payload expected\n");
    else if (can_message_type == 2) {
        if (frame_rd.can_dlc == 4) {
            uint16_t value = (frame_rd.data[1] << 8) | frame_rd.data[0];
            fprintf(stdout, " ... Response: Value=0x%04X (%d)\n", value, value);
        } else if (frame_rd.can_dlc == 7) {
            uint16_t value = (frame_rd.data[1] << 8) | frame_rd.data[0];
            fprintf(stdout, " ... Response: Value=0x%04X (%d)\n", value, value);
        } else if (frame_rd.can_dlc == 8) {
            uint16_t value = (frame_rd.data[1] << 8) | frame_rd.data[0];
            fprintf(stdout, " ... Response: Value=0x%04X (%d)\n", value, value);
        } else {
            fprintf(stdout, " ... Payload (unknown format): ");
            for (int i = 0; i < frame_rd.can_dlc; i++)
                fprintf(stdout, "0x%02X ", frame_rd.data[i]);
            fprintf(stdout, "\n");
        }
    } else {
        fprintf(stdout, " ... Payload: ");
        for (int i = 0; i < frame_rd.can_dlc; i++)
            fprintf(stdout, "0x%02X ", frame_rd.data[i]);
        fprintf(stdout, "\n");
    }
}





#if 0

else if (program_type == 0x8B) {
    const char* func_type_str = "UNKNOWN";
    switch (function_type) {
        case 0x00: func_type_str = "General_Status"; break;
        case 0x01: func_type_str = "DLF_SENSOR"; break;
        case 0x02: func_type_str = "DLF_RELAY"; break;
        case 0x08: func_type_str = "DLG_HYDRAULIK_CONFIG"; break;
    }
    
    fprintf(stdout, " ... HEATINGCONTROL (0x8B): Function=0x%02X (%s), MessageType=0x%X (%s), SubscriberID=0x%02X\n", function_type, func_type_str, can_message_type, msg_type_str, subscriber_id);
    
    if (frame_rd.can_dlc > 0) {
        // For DLF_SENSOR (0x01)
        if (function_type == 0x01) {
            uint8_t sensor_no = frame_rd.data[0];
            uint16_t value = (frame_rd.data[2] << 8) | frame_rd.data[1];
            fprintf(stdout, "    Sensor: Number=0x%02X, Value=0x%04X", sensor_no, value);
            if (frame_rd.can_dlc >= 4) {
                uint8_t sensor_type = frame_rd.data[3];
                fprintf(stdout, ", Type=0x%02X", sensor_type);
            }
            if (frame_rd.can_dlc >= 8) 
                fprintf(stdout, ", Flags=0x%02X", frame_rd.data[7]);
            fprintf(stdout, "\n");
        }
        // For DLF_RELAY (0x02)
        else if (function_type == 0x02) {
            uint8_t relay_no = frame_rd.data[0];
            uint8_t mode = frame_rd.data[1];
            uint16_t value = (frame_rd.data[3] << 8) | frame_rd.data[2];
            fprintf(stdout, "    Relay: Number=0x%02X, Mode=0x%02X, Value=0x%04X", relay_no, mode, value);
            if (frame_rd.can_dlc >= 5) {
                fprintf(stdout, ", ExtraData=");
                for (int i = 4; i < frame_rd.can_dlc; i++) 
                    fprintf(stdout, "%02X", frame_rd.data[i]);
            }
            fprintf(stdout, "\n");
        }
        // For DLG_HYDRAULIK_CONFIG (0x08)
        else if (function_type == 0x08) {
            fprintf(stdout, "    Config: ");
            for (int i = 0; i < frame_rd.can_dlc; i++) 
                fprintf(stdout, "0x%02X ", frame_rd.data[i]);
            fprintf(stdout, "\n");
        }
        // Generic payload dump for other types
        else {
            fprintf(stdout, "    Payload: ");
            for (int i = 0; i < frame_rd.can_dlc; i++) 
                fprintf(stdout, "0x%02X ", frame_rd.data[i]);
            fprintf(stdout, "\n");
        }
    }
}

#endif
else {
	fprintf (stdout, "\n");
}


		    fflush(stdout);
                }
            }
        }
    }
}

int close_port()
{
    close(soc);
    return 0;
}

int main(void)
{
    open_port("vcan0");
    read_port();
    return 0;
}
