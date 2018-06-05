tm = {
    "6102000000000000": ("210000", "采掘"),
    "6103000000000000": ("220000", "化工"),
    "6104000000000000": ("230000", "钢铁"),
    "6105000000000000": ("240000", "有色金属"),
    "6106010000000000": ("610000", "建筑材料"),
    "6106020000000000": ("620000", "建筑装饰"),
    "6107010000000000": ("630000", "电气设备"),
    "6107000000000000": ("640000", "机械设备"),
    "1000012579000000": ("650000", "国防军工"),
    "1000012588000000": ("280000", "汽车"),
    "6111000000000000": ("330000", "家用电器"),
    "6113000000000000": ("350000", "纺织服装"),
    "6114000000000000": ("360000", "轻工制造"),
    "6120000000000000": ("450000", "商业贸易"),
    "6101000000000000": ("110000", "农林牧渔"),
    "6112000000000000": ("340000", "食品饮料"),
    "6121000000000000": ("460000", "休闲服务"),
    "6115000000000000": ("370000", "医药生物"),
    "6116000000000000": ("410000", "公用事业"),
    "6117000000000000": ("420000", "交通运输"),
    "6118000000000000": ("430000", "房地产"),
    "6108000000000000": ("270000", "电子"),
    "1000012601000000": ("710000", "计算机"),
    "6122010000000000": ("720000", "传媒"),
    "1000012611000000": ("730000", "通信"),
    "1000012612000000": ("480000", "银行"),
    "1000012613000000": ("490000", "非银金融"),
    "6123000000000000": ("510000", "综合"),
}


class Sector:
    def __init__(self):
        self.shsz = {
            "ab_all": "a001010900000000",   # 全部AB股
            "sshkc": "1000025141000000",    # 陆股通
            "shhkc": "1000014938000000",    # 沪股通
            "szhkc": "1000023475000000",    # 深股通
            "listed": "a001010c00000000"    # 全部上市公司
        }
