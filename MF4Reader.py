from asammdf import MDF
from asammdf.mdf import MdfException
from can.io.blf import BLFReader
from dbcparser import Database, Message, Signal
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.ticker as tck
import argparse
from numpy import isnan
from dataclasses import dataclass


class MF4Reader:
    def __init__(self, log_folder: Path, dbc_folder: Path):
        self.can_channels = []
        self.database = None
        self.figure = None
        self._plot_idx = 1
        self._plot_signal_list = []
        self.msg_frames = []

        # parse dbc files
        for fp in dbc_folder.iterdir():
            if fp.is_file() and fp.suffix.lower() == '.dbc':
                if self.database is None:
                    self.database = Database(fp)
                else:
                    # merge into one file
                    self.database.merge(Database(fp))

        # get list of all messages
        msg_list = []
        for dbc_msg in self.database.messages:
            if Message.is_pdu1(dbc_msg.id):
                msg_list.append(self.MessageLogPdu1(dbc_msg))
            else:
                msg_list.append(self.MessageLog(dbc_msg))

        unknown_list = []

        tg0 = None
        for fp in log_folder.iterdir():
            if not fp.is_file():
                continue
            if fp.suffix.lower() == '.mf4':
                log_f = MDF(fp)
                if tg0 is None:
                    # set 'global' T0
                    tg0 = log_f.start_time.timestamp()
                # log file specific T0
                tl0 = log_f.start_time.timestamp()

                for can_n in log_f.bus_logging_map['CAN'].keys():
                    self.can_channels.append(can_n)

                # iter over groups
                for i in range(len(log_f.virtual_groups)):
                    pnds_arr = log_f.get_group(i)
                    if len(pnds_arr) == 0:
                        # skip?
                        continue

                    log_len = pnds_arr.shape[0]
                    data_bytes = pnds_arr['CAN_DataFrame.CAN_DataFrame.DataBytes'].values
                    bus_channels = pnds_arr['CAN_DataFrame.CAN_DataFrame.BusChannel'].values
                    msg_ids = pnds_arr['CAN_DataFrame.CAN_DataFrame.ID'].values
                    log_timestamps = pnds_arr.axes[0].values

                    for idx in range(log_len):
                        # time_stamp = __has_timestamp(log_timestamps[idx])
                        # if time_stamp is None:
                        #     time_stamp = MF4Reader.CanTimeStamp(log_timestamps[idx])
                        #     self.timeline.append(time_stamp)

                        # skip missing data
                        if isnan(msg_ids[idx]):
                            continue

                        # get message class
                        msg_id, msg_sa = Message.get_pgn(int(msg_ids[idx]))
                        if Message.is_pdu1(int(msg_ids[idx])):
                            # zero DA
                            msg_da = msg_id & 0xFF
                            msg_id = msg_id & 0x1FFF00
                        else:
                            msg_da = None

                        if msg_id in unknown_list:
                            continue

                        # data
                        data = list(data_bytes[idx])
                        # data_tmp = data_bytes[idx]
                        # data: int = 0
                        # for d_idx in range(len(data_tmp)):
                        #     data = data + (data_tmp[d_idx] * 2**(d_idx * 8))

                        # channel
                        channel = bus_channels[idx]

                        # timestamp
                        t = tl0 - tg0 + log_timestamps[idx]

                        # append
                        for msg_log in msg_list:
                            if msg_log.get_pgn() == msg_id:
                                if msg_da is None:
                                    msg_log.add_frame(t, data, msg_sa, channel)
                                else:
                                    msg_log.add_frame(t, data, msg_da, msg_sa, channel)
                                break
                        else:
                            # append unknown list
                            unknown_list.append(msg_id)
                log_f.close()

                # log_f.bus_logging_map['CAN'] - dictionary with num of CAN? and inside {msg ID: group_id}
            elif fp.suffix.lower() == '.blf':
                log_f = BLFReader(fp)
                if tg0 is None:
                    # set 'global' T0
                    tg0 = log_f.start_timestamp
                # log file specific T0
                # tl0 = log_f.start_timestamp

                for msg in log_f:
                    msg_sa = self.sa_from_id(msg.arbitration_id)
                    msg_id = self.pgn_from_id(msg.arbitration_id, with_priority=True)
                    if Message.is_pdu1(msg.arbitration_id):
                        # zero DA
                        msg_da = msg_id & 0xFF
                        msg_id = msg_id & 0x1FFF00
                    else:
                        msg_da = None

                    if msg_id in unknown_list:
                        continue

                    channel = msg.channel
                    # data
                    data = list(msg.data)
                    # timestamp
                    t = msg.timestamp - tg0
                    # append
                    for msg_log in msg_list:
                        if msg_log.get_pgn() == msg_id:
                            if msg_da is None:
                                msg_log.add_frame(t, data, msg_sa, channel)
                            else:
                                msg_log.add_frame(t, data, msg_da, msg_sa, channel)
                            break
                    else:
                        # append unknown list
                        unknown_list.append(msg_id)

        # filter empty messages
        self.msg_frames = [x for x in msg_list if not x.is_empty()]

    @staticmethod
    def sa_from_id(msg_id):
        return msg_id & 0xFF

    @staticmethod
    def pgn_from_id(msg_id, with_priority=False):
        if with_priority:
            return (msg_id >> 8) & 0x1FFFFF
        else:
            return (msg_id >> 8) & 0x3FFFF

    def get_message(self, msg_name: str) -> 'MF4Reader.TraceData | None':
        for msg_log in self.msg_frames:
            frame_trace = msg_log.get_frame_trace(msg_name)
            if frame_trace is not None:
                return frame_trace
        else:
            return None

    @staticmethod
    def __plot(ax, t, x, signal: Signal, title: str):
        ax.step(t, x, 'o-', where='post')
        title = signal.name + title
        ax.set_title(title)
        ax.xaxis.set_minor_locator(tck.AutoMinorLocator())
        ax.grid(visible=True, which='major')
        ax.grid(visible=True, which='minor', axis='x')

        # customise Y
        if signal.value_table is not None:
            y_vals = []
            y_labels = []
            for val in signal.value_table.table.keys():
                label = signal.value_table.table[val]
                for i in range(len(y_vals)):
                    if val > y_vals[i]:
                        y_vals.insert(i, val)
                        y_labels.insert(i, label)
                        break
                else:
                    y_vals.insert(0, val)
                    y_labels.insert(0, label)

            ax.set_yticks(y_vals, labels=y_labels)
        elif signal.length == 1:
            ax.set_yticks([0, 1], labels=['False', 'True'])

    def __clear_fig(self, *args):
        self.figure = None
        self._plot_idx = 1
        self._plot_signal_list = []

    def remove_axes(self, index: int = None):
        if self.figure is None:
            # nothing to remove
            return
        axes_range = len(self.figure.get_axes())
        if axes_range == 0:
            # no axes in figure, just exit
            return
        if index is None:
            # remove the last axes
            index = axes_range - 1
        else:
            if index < 0 or index >= axes_range:
                raise IndexError(f'Axes index {index} is out of bounds')

        if axes_range == 1:
            # last axes, just close the figure
            plt.close(self.figure)
            return

        # remove given axes
        self._plot_idx -= 1
        del self._plot_signal_list[index]
        self.__refresh_plot(self._plot_idx-1)

    def __refresh_plot(self, new_size):
        if len(self.figure.axes) != new_size:
            old_axes = self.figure.get_axes()
            for i, ax in enumerate(old_axes):
                old_axes[i] = [ax.lines[0].get_xdata(), ax.lines[0].get_ydata()]
            self.figure.clear()
            new_axes = self.figure.subplots(new_size, 1)
            copy_range = min(len(old_axes), new_size)
            for i in range(copy_range):
                ax = old_axes[i]
                self.__plot(self.figure.axes[i], ax[0], ax[1], self._plot_signal_list[i][0], self._plot_signal_list[i][1])
            return new_axes
        else:
            return self.figure.axes

    def __append_figure(self, t, y, signal, title):
        # plot
        plt.ion()
        if self.figure is None:
            self.figure = plt.figure()
            self.figure.canvas.mpl_connect('close_event', self.__clear_fig)
            self._plot_idx = 1
            new_axes = self.figure.subplots(1, 1)
            self.__plot(new_axes, t, y, signal, title)
            self._plot_signal_list = [(signal, title)]
        else:
            new_axes = self.__refresh_plot(self._plot_idx)
            self.__plot(new_axes[self._plot_idx - 1], t, y, signal, title)
            self._plot_signal_list.append((signal, title))
        plt.show()
        # increment num of plots
        self._plot_idx += 1

    def plot_signal(self, msg_name, sig_name=None):
        # get signal data
        try:
            dbc_msg = self.database.get_message(msg_name)
            if dbc_msg is None:
                print('No such message: ' + msg_name)
                return

            if sig_name is not None:
                dbc_sig = dbc_msg.get_signal(sig_name)
                if dbc_sig is None:
                    print('No such signal: ' + sig_name)
                    return
                dbc_sig = [dbc_sig]
            else:
                # get all signals
                dbc_sig = dbc_msg.signals

            # fetch all message frames
            trace_data = self.get_message(msg_name)
            if trace_data is None:
                print(f'No {msg_name} message in the logs.')
                return

            # draw
            for sig in dbc_sig:
                Y_data = []
                T_data = []
                # sort, just in case
                trace_data.trace.sort(key=lambda x: x[0])
                for msg_data in trace_data.trace:
                    Y_data.append(sig.bytes2data(msg_data[1]))
                    T_data.append(msg_data[0])

                self.__append_figure(T_data, Y_data, sig, trace_data.to_title())

            plt.draw()
            plt.pause(0.1)
        except MdfException as ex:
            print(ex)

    def plot_dtc(self, spn, fmi):
        # find sig and msg
        dtc_active_msg = 'DM01'
        sig_PL = 'PLStatus'  # Protection Lamp
        sig_AWL = 'AWLStatus'  # Amber        Warning        Lamp
        sig_RLS = 'RSLState'  # Red        Stop        Lamp
        sig_MIL = 'MILStatus'  # Malfunction        Indicator        Lamp
        # FlashAmberWarningLamp ???
        # FlashMalfuncIndicatorLamp ???
        # FlashProtectLamp ???
        # FlashRedStopLamp ???

        dbc_msg = self.database.get_message(dtc_active_msg)
        if dbc_msg is None:
            print('No prototype for DTC message found')
            return
        dtc_lamps =[dbc_msg.get_signal(sig_PL),
                    dbc_msg.get_signal(sig_AWL),
                    dbc_msg.get_signal(sig_RLS),
                    dbc_msg.get_signal(sig_MIL)]
        dtc_signals = [dbc_msg.get_signal('DTC1'),
                       dbc_msg.get_signal('DTC2'),
                       dbc_msg.get_signal('DTC3'),
                       dbc_msg.get_signal('DTC4'),
                       dbc_msg.get_signal('DTC5')]
        if None in dtc_lamps or None in dtc_signals:
            print('Unsupported DTC frame format. Missing signals')
            return

        # fetch all message frames
        trace_data = self.get_message(dtc_active_msg)
        if trace_data is None:
            print(f'No DTC active message in the logs.')
            return

        y_data = dict()
        for lamp in dtc_lamps:
            y_data[lamp.name] = []
        t_data = []
        for entry in trace_data.trace:
            # check if contains correct DTC
            dtc_list = []
            for dts_sig in dtc_signals:
                dtc = dts_sig.bytes2data(entry[1])
                dtc = int(dtc)
                if dtc != 0x0:  # dtc != 0xFFFF_FFFF
                    dtc_list.append(dtc)

            for dtc in dtc_list:
                # remove CM and OC(4th byte)
                tmp_val = dtc & 0xFF_FFFF
                # get FMI
                dtc_fmi = (tmp_val & 0x1F_0000) >> 16
                # restore SPN(4th method, new)
                dtc_spn = dtc & 0xFFFF
                dtc_spn = dtc_spn + ((dtc & 0xE0_0000) >> 5)

                # compare
                if dtc_spn == spn and dtc_fmi == fmi:
                    # found DTC of interest
                    t_data.append(entry[0])
                    for lamp in dtc_lamps:
                        y_data[lamp.name].append(lamp.bytes2data(entry[1]))
        # draw
        for lamp in dtc_lamps:
            self.__append_figure(t_data, y_data[lamp.name], lamp, trace_data.to_title())

        plt.draw()
        plt.pause(0.1)

    def get_messages_from_source(self, source_address: int) -> list:
        out = []
        for msg_log in self.msg_frames:
            if msg_log.msg not in out:
                if msg_log.has_sa(source_address):
                    out.append(msg_log.msg)
        return out

    @dataclass
    class TraceData:
        trace: list = None
        # frame: Message
        SA: int = None
        DA: int = None
        CAN: int = None

        def to_title(self) -> str:
            if self.SA is None and self.DA is None and self.CAN is None:
                return ''
            else:
                out = []
                if self.SA is not None:
                    out.append(f'SA:{self.SA:X}')
                if self.DA is not None:
                    out.append(f'DA:{self.DA:X}')
                if self.CAN is not None:
                    out.append(f'CAN:{self.CAN}')
                return '(' + ' '.join(out) + ')'

        @staticmethod
        def select_sa(src_list: list) -> int:
            print('Select SA:\n')
            for v in src_list:
                print(f'0x{v:X}\n')
            sa_key = input('')
            if sa_key.lower().startswith('0x'):
                sa_key = int(sa_key, 16)
            else:
                sa_key = int(sa_key)
            return sa_key

        @staticmethod
        def select_da(dst_list: list) -> int:
            print('Select DA:\n')
            for v in dst_list:
                print(f'0x{v:X}\n')
            da_key = input('')
            if da_key.lower().startswith('0x'):
                da_key = int(da_key, 16)
            else:
                da_key = int(da_key)
            return da_key

        @staticmethod
        def select_can(channel_list: list) -> int:
            # TODO display real CAN names?
            print('Select CAN:\n')
            for v in channel_list:
                print(f'{v}\n')
            can_key = int(input(''))
            return can_key

    class MsgDestination:
        def __init__(self, da: int):
            self.address: int = da
            self._sources: dict[int, MF4Reader.MsgSource] = dict()

        def add_frame(self, time, data, sa, channel):
            if sa not in self._sources:
                self._sources[sa] = MF4Reader.MsgSource(sa)
            self._sources[sa].add_frame(time, data, channel)

        def has_sa(self, sa: int):
            return sa in self._sources

        def get_trace(self, trace_data: 'MF4Reader.TraceData' = None) -> 'MF4Reader.TraceData':
            # create output struct
            if trace_data is None:
                trace_data = MF4Reader.TraceData()
            else:
                # reuse mutable input
                pass

            if len(self._sources) > 1:
                if trace_data.SA is None:
                    sa_key = MF4Reader.TraceData.select_sa(list(self._sources.keys()))
                    trace_data.SA = sa_key
                else:
                    sa_key = trace_data.SA
            else:
                sa_key = list(self._sources.keys())[0]
                trace_data.SA = None
            return self._sources[sa_key].get_trace(trace_data=trace_data)

    class MsgSource:
        def __init__(self, sa: int):
            self.address: int = sa

            self.channels: dict[int, list] = dict()

        def add_frame(self, time: float, data, channel: int):
            if channel not in self.channels:
                self.channels[channel] = [(time, data)]
            else:
                self.channels[channel].append((time, data))

        def get_trace(self, trace_data: 'MF4Reader.TraceData' = None) -> 'MF4Reader.TraceData':
            # create output struct
            if trace_data is None:
                trace_data = MF4Reader.TraceData()
            else:
                # reuse mutable input
                pass

            # select CAN
            if len(self.channels) > 1:
                if trace_data.CAN is None:
                    can_key = MF4Reader.TraceData.select_can(list(self.channels.keys()))
                    trace_data.CAN = can_key
                else:
                    can_key = trace_data.CAN
            else:
                can_key = list(self.channels.keys())[0]
                trace_data.CAN = None
            trace_data.trace = self.channels[can_key]
            return trace_data

    class MessageLog:
        def __init__(self, msg_obj):
            self.msg = msg_obj

            self._sources: dict[int, MF4Reader.MsgSource] = dict()

        def add_frame(self, time: float, data, sa: int, channel):
            if sa not in self._sources:
                self._sources[sa] = MF4Reader.MsgSource(sa)
            self._sources[sa].add_frame(time, data, channel)

        def get_pgn(self):
            return self.msg.pgn

        def has_sa(self, sa: int):
            return sa in self._sources

        def get_frame_trace(self, msg_name: str) -> 'MF4Reader.TraceData | None':
            if self.msg.name == msg_name:
                trace_data = MF4Reader.TraceData()
                # select source
                if len(self._sources) > 1:
                    sa_key = MF4Reader.TraceData.select_sa(list(self._sources.keys()))
                    # trace_data.SA = sa_key
                else:
                    sa_key = list(self._sources.keys())[0]
                    # trace_data.SA = None
                trace_data.SA = sa_key
                return self._sources[sa_key].get_trace(trace_data=trace_data)
            else:
                return None

        def is_empty(self) -> bool:
            return len(self._sources) == 0

    class MessageLogPdu1:
        def __init__(self, msg_obj):
            self.msg = msg_obj
            self._destinations: dict[int, 'MF4Reader.MsgDestination'] = dict()

        def add_frame(self, time: float, data, da: int, sa, channel):
            if da not in self._destinations:
                self._destinations[da] = MF4Reader.MsgDestination(da)
            self._destinations[da].add_frame(time, data, sa, channel)

        def get_pgn(self):
            # zero DA
            return self.msg.pgn & 0x1FFF00

        def has_sa(self, sa: int):
            for da in self._destinations:
                if self._destinations[da].has_sa(sa):
                    return True
            return False

        def get_frame_trace(self, msg_name: str) -> 'MF4Reader.TraceData | None':
            if self.msg.name == msg_name:
                trace_data = MF4Reader.TraceData()
                # select source
                if len(self._destinations) > 1:
                    da_key = MF4Reader.TraceData.select_da(list(self._destinations.keys()))
                    trace_data.DA = da_key
                else:
                    da_key = list(self._destinations.keys())[0]
                    trace_data.DA = None
                return self._destinations[da_key].get_trace(trace_data=trace_data)
            else:
                return None

        def is_empty(self) -> bool:
            return len(self._destinations) == 0


if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='This tool can read can log file(s) and plot data using dbc files.')
    aparser.add_argument('-dbc', nargs='?', help='Folder with dbc files')
    aparser.add_argument('-l', '--logs', nargs='?', help='Folder with MF4 log files')
    args = aparser.parse_args()

    if args.logs is None:
        args.logs = input('Log files folder: ')
    args.logs = Path(args.logs)

    if args.dbc is None:
        args.dbc = input('DBC folder: ')
    args.dbc = Path(args.dbc)

    mReader = MF4Reader(args.logs, args.dbc)

    while True:
        sig_name = input('Signal to add (<Msg.Sig>):')
        if sig_name == '':
            break

        sig_name = sig_name.split('.')
        msg_name = sig_name[0]
        if msg_name == '-1':
            mReader.remove_axes()
            continue
        if len(sig_name) == 1:
            sig_name = None
        elif sig_name[0].lower() == 'dtc':
            if len(sig_name) != 3:
                print('Bad DTC format. Expected: DTC.SPN.FMI')
                continue
            spn = int(sig_name[1])
            fmi = int(sig_name[2])
            mReader.plot_dtc(spn, fmi)
        else:
            sig_name = sig_name[1]
            mReader.plot_signal(msg_name, sig_name=sig_name)
