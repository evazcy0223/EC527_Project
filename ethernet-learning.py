from pox.core import core
import pox.openflow.libopenflow_01 as of

log = core.getLogger()


# data structure 
mac_to_port = {}

# global variables for accumulating the count of floods and the count of packets 
flood_count = 0
packet_count = 0

def resend_packet(event, packet_in, out_port):
    """
    Instructs the switch to resend a packet that it had sent to us.
    "packet_in" is the ofp_packet_in object the switch had sent to the
    controller due to a table-miss.
    """
    msg = of.ofp_packet_out()

    # set the data 
    msg.data = packet_in

    # add an action to send to the specified port
    action = of.ofp_action_output(port=out_port)
    msg.actions.append(action)

    # send message to switch
    event.connection.send(msg)


def _handle_PacketIn (event):
    global flood_count
    global packet_count 
    # accumulate the count of the packets 
    packet_count += 1

    # get the port the packet came in on for the switch contacting the controller
    packetInPort = event.port

    # use POX to parse the packet
    packet = event.parsed

    # get src and dst mac addresses
    src_mac = str(packet.src)
    dst_mac = str(packet.dst)

    # get switch ID
    switchID = str(event.connection.dpid) + str(event.connection.ID)
    log.info('Packet has arrived: SRCMAC:{} DSTMAC:{} from switch:{} in-port:{}'.format(src_mac, dst_mac, switchID, packetInPort))

    # ofp_packet_in object the switch had sent to the controller due to a table-miss.
    packet_in = event.ofp

    # Update the mac_to_port so that it records the port number to reach the source host 
    try:
        if not (src_mac in mac_to_port[switchID].keys()):
            mac_to_port[switchID][src_mac] = packetInPort
            msg = of.ofp_flow_mod()
            msg.match = of.ofp_match(dl_dst = packet.src)
            # set the action of the flow entry to send to the specific port 
            msg.actions.append(of.ofp_action_output(port = mac_to_port[switchID][src_mac]))
            msg.data = packet_in
            event.connection.send(msg)
    except KeyError:
        mac_to_port[switchID] = {src_mac: packetInPort}
        msg = of.ofp_flow_mod()
        msg.match = of.ofp_match(dl_dst = packet.src)
        # set the action of the flow entry to send to the specific port 
        msg.actions.append(of.ofp_action_output(port = mac_to_port[switchID][src_mac]))
        msg.data = packet_in
        event.connection.send(msg)

    if dst_mac in mac_to_port[switchID].keys():
        # if the destination MAC address is in the keys of the mac_to_port[current switch], 
        # push a flow entry about the destination host based on the matching of destination MAC address 
        # and send the packet 

        log.info('Installing flow to switch:{}... DSTMAC:{} in-port:{}'.format(switchID, dst_mac, mac_to_port[switchID][dst_mac]))
        # Maybe the log statement should have source/destination/port?

        log.info("I know it")

        msg = of.ofp_flow_mod()
        msg.match = of.ofp_match(dl_dst = packet.dst)
        # set the action of the flow entry to send to the specific port 
        msg.actions.append(of.ofp_action_output(port = mac_to_port[switchID][dst_mac]))
        # set the data 
        msg.data = packet_in

        event.connection.send(msg)

    else:
        # if the destination MAC address is not in the keys of the mac_to_port[current switch], 
        # flood the packet to all the ports except for the input port 

        # increase the count of floods by the number of ports at a switch - 1
        flood_count += (len(event.connection.ports) - 2)

        log.info("I ganna flood {}".format(flood_count))
        # flood the packet 
        resend_packet(event, packet_in, of.OFPP_FLOOD)

    log.info(mac_to_port)
    log.info(packet_count)

def launch ():
    core.openflow.addListenerByName("PacketIn", _handle_PacketIn)
    log.info("Pair-Learning switch running.")
