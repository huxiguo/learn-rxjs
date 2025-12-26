import { Injectable } from '@nestjs/common'
import {
    Asset,
    AssetServicePeriodOrder,
    DeviceBindRule,
    DevicePackage,
    DevicePackageProfitSharingRule,
    DeviceRechargeRule,
    Prisma,
    Tracker,
    TrackerModel,
    User
} from '@prisma/client'
import { Dayjs } from 'dayjs'
import * as O from 'fp-ts/Option'
import { createUUID } from 'src/common/hash-helper'
import { isNullOrEmptyString } from 'src/common/string-helper'
import { getNow, parseDate } from 'src/common/time-helper'
import { ActivityEnums, AlarmTypeEnums, AssetOrderStatusEnums, OrderTargetEnums } from 'src/dto/consts'
import { calculateEndTime, calculateServiceEndTimeForRenew } from 'src/dto/logic/calculate'
import { AlarmConfigService } from './alarm-config.service'
import { CdcMqService } from './cdc-mq.service'
import { PrismaService } from './prisma-service'
import { ProfitSharingService } from './profit-sharing-service'
import { TrackerStatusService } from './tracker-status.service'

type DevicePackageWithModel = DevicePackage & { deviceBindRule: DeviceBindRule | null } & {
    deviceRechargeRules: (DeviceRechargeRule & { devicePackageProfitSharingRules: DevicePackageProfitSharingRule[] })[]
} & {
    trackereModels: TrackerModel[]
}

type TrackerWithAsset = Tracker & { asset: Asset }

type DeviceWithDevicePackageAndOpenRule = {
    tracker: TrackerWithAsset
    devicePackage: DevicePackageWithModel
    openRule: { chargeDuration: number; chargeTimeUnit: string }
}

type DeviceWithServiceTimeRange = { tracker: TrackerWithAsset } & {
    serviceTimeRange: { startTime: Dayjs; endTime: Dayjs } | null
}

@Injectable()
export class AssetService {
    constructor(
        private readonly prisma: PrismaService,
        private readonly alarmConfig: AlarmConfigService,
        private readonly cdc: CdcMqService,
        private readonly profitSharingService: ProfitSharingService,
        private readonly deviceStatusService: TrackerStatusService
    ) { }
    async batchQueryTrackerAsset(trackers: Tracker[]): Promise<Asset[]> {
        const trackerIds = trackers.map(x => x.id)

        const userAssets = await this.prisma.asset.findMany({ where: { trackerId: { in: trackerIds } } })

        return userAssets
    }

    // 构建多个设备与设备套餐的关系
    buildTrackersWithPackageRelation(
        trackerWithAsset: TrackerWithAsset[],
        devicePackages: DevicePackageWithModel[]
    ): { tracker: TrackerWithAsset; devicePackage: DevicePackageWithModel | null }[] {
        const result: { tracker: TrackerWithAsset; devicePackage: DevicePackageWithModel | null }[] = []

        // 遍历所有设备，找到对应的设备套餐
        trackerWithAsset.forEach(device => {
            // 根据设备的商户id和设备型号id，找到对应的设备套餐
            const devicePackage = this.findDevicePackageByDevice(device, devicePackages)

            // 将设备和设备套餐关联
            result.push({ tracker: device, devicePackage: devicePackage ?? null })
        })

        return result
    }

    findDevicePackageByDevice(trackerWithAsset: TrackerWithAsset, devicePackages: DevicePackageWithModel[]): DevicePackageWithModel | null {
        // 首先找到商户资产对应的商户的设备套餐
        const merchantDevicePackages = devicePackages.find(devicePackage => {
            const isMerchantMatch = devicePackage.merchantId === trackerWithAsset.asset.merchantId
            const isModelMatch = devicePackage.trackereModels.some(deviceModel => deviceModel.id === trackerWithAsset.trackerModelId)
            return isMerchantMatch && isModelMatch
        })

        // 如果找到了商户的设备套餐，则返回
        if (merchantDevicePackages != null) {
            return merchantDevicePackages
        }

        // 如果没有找到商户的设备套餐，则查找平台的设备套餐
        return (
            devicePackages.find(devicePackage => {
                const isPlatformMatch = devicePackage.merchantId == null
                const isModelMatch = devicePackage.trackereModels.some(deviceModel => deviceModel.id === trackerWithAsset.trackerModelId)
                return isPlatformMatch && isModelMatch
            }) ?? null
        )
    }

    // 选择合适的设备套餐的开通规则
    chooseOpenRule(devicePackage: DevicePackageWithModel): { chargeDuration: number; chargeTimeUnit: string } | null {
        if (devicePackage.isRelatedOpen) {
            if (devicePackage.deviceBindRule == null) {
                return null
            }

            return {
                chargeDuration: devicePackage.deviceBindRule.chargeDuration,
                chargeTimeUnit: devicePackage.deviceBindRule.chargeTimeUnit
            }
        }

        const openRules = devicePackage.deviceRechargeRules.filter(x => x.isOpeningRule)

        if (openRules.length == 0) {
            return null
        }

        return {
            chargeDuration: openRules[0].chargeDuration,
            chargeTimeUnit: openRules[0].chargeTimeUnit
        }
    }

    async bindDeviceToUser(accountId: string, deviceWithPackage: DeviceWithDevicePackageAndOpenRule, user: User): Promise<void> {
        return this.bindDevicesToUser(accountId, [deviceWithPackage], user)
    }

    async bindDevicesToUser(accountId: string, devicesWithPackage: DeviceWithDevicePackageAndOpenRule[], user: User): Promise<void> {
        const devicesWithServiceTimeRange = devicesWithPackage.map(value => {
            return {
                tracker: value.tracker,
                serviceTimeRange: value.devicePackage.isRelatedOpen
                    ? this.calculateServiceTimeRangeForOpen(
                        value.tracker,
                        {
                            duration: value.openRule.chargeDuration,
                            timeUnit: value.openRule.chargeTimeUnit
                        },
                        null
                    )
                    : null
            }
        })

        await this.bindDeviceToUserCore(accountId, devicesWithServiceTimeRange, user.id)
    }

    private async bindDeviceToUserCore(
        accountId: string,
        devicesWithServiceTimeRange: DeviceWithServiceTimeRange[],
        userId: string
    ): Promise<void> {
        // 创建事务数组
        const tasks: Prisma.PrismaPromise<any>[] = []
        const nowtime = getNow().toDate()

        // 判断中控是否被激活了，中控激活的标准是，是否存在资产服务结束时间
        // 先使用trackerId查询资产里面的服务结束时间是否已经存在
        // 如果存在则将用户id赋给资产
        // 如果不存在则将规则时间和用户id一起赋给资产
        // 在绑定中控时为中控绑定时间赋值
        devicesWithServiceTimeRange.forEach(x => {
            if (x.tracker.asset.serviceEndTime != null) {
                // 已激活绑定
                tasks.push(this.prisma.asset.update({
                    where: {
                        id: x.tracker.asset.id
                    },
                    data: {
                        userId: userId,
                        serviceStartTime: nowtime,
                        trackerBoundAt: nowtime
                    }
                }))
            } else {
                // 未激活绑定
                // todo 未考虑付费后开通
                tasks.push(this.prisma.asset.update({
                    where: {
                        id: x.tracker.asset.id
                    },
                    data: {
                        userId: userId,
                        serviceStartTime: nowtime,
                        serviceEndTime: x.serviceTimeRange!.endTime.toDate(),
                        trackerBoundAt: nowtime
                    }
                }))
            }

            // todo 在中控分配商户时创建校准历史
            // 创建里程校准历史
            // 如果有商户里程校准 那么里程校准历史就使用商户的里程校准
            // 如果没有商户里程校准 那么就默认设置为1
            // 时间值只精确到小时
            // tasks.push(this.prisma.mileageCalibrationHistory.create({
            //     data: {
            //         id: createUUID(),
            //         // 现在没有商户预设的里程校准，所以默认设置为1
            //         mileageCalibration: 1,
            //         // 时间值只精确到小时
            //         effectiveTime: getNow().minute(0).second(0).millisecond(0).toDate(),
            //         createdBy: accountId,
            //         assetId: x.tracker.asset.id
            //     }
            // }))

            Object.values(AlarmTypeEnums).forEach(alarmType => {
                tasks.push(this.prisma.assetNotificationConfig.create({
                    data: {
                        id: createUUID(),
                        kind: alarmType,
                        appNotification: false,
                        smsNotification: false,
                        phoneNotification: false,
                        wechatNotification: false,
                        phoneAlarmStartTime: '00:00',
                        phoneAlarmEndTime: '23:59',
                        smsAlarmStartTime: '00:00',
                        smsAlarmEndTime: '23:59',
                        createdAt: getNow().toDate(),
                        createdBy: accountId,
                        assetId: x.tracker.asset.id,
                        updatedAt: getNow().toDate(),
                        updatedBy: accountId
                    }
                }))
            })
        })

        await this.prisma.$transaction(tasks)


        const createAlarmConfigTasks = devicesWithServiceTimeRange.map(value => {
            return this.alarmConfig.refresh(value.tracker.asset.id)
        })

        // 删除设备状态
        const devicesForClearStatus = devicesWithServiceTimeRange.map(value => ({
            id: value.tracker.id,
            trackerNumber: value.tracker.trackerNumber
        }))

        await Promise.all([...createAlarmConfigTasks, this.deviceStatusService.batchClearRealtimeStatus(devicesForClearStatus)])

        await this.cdc.sendToCDCBatch(
            devicesWithServiceTimeRange.map(x => ({
                kind: 'device-changed',
                id: x.tracker.id
            }))
        )
    }

    // 获取设备套餐
    async getDevicePackages(merchantIds: string[]): Promise<DevicePackageWithModel[]> {
        // 查询商户与平台的设备套餐
        const devicePackages = await this.prisma.devicePackage.findMany({
            where: {
                OR: [
                    {
                        merchantId: {
                            in: merchantIds
                        }
                    },
                    {
                        merchantId: null
                    }
                ]
            },
            include: {
                deviceBindRule: true,
                deviceRechargeRules: {
                    include: {
                        devicePackageProfitSharingRules: true
                    }
                }
            }
        })

        // 查询关系
        const relations = await this.prisma.devicePackageWithDeviceModelRelation.findMany({
            where: {
                devicePackageId: {
                    in: devicePackages.map(x => x.id)
                }
            }
        })

        // 根据关系查询设备型号
        const trackerModels = await this.prisma.trackerModel.findMany({
            where: {
                id: {
                    in: relations.map(x => x.deviceModelId)
                }
            }
        })

        // 将设备型号和设备套餐关联
        return devicePackages.map(item => {
            const models = relations
                .filter(relation => relation.devicePackageId === item.id)
                .map(relation => trackerModels.find(x => x.id === relation.deviceModelId))
                .filter(x => x != null)
                .map(x => x!)

            return {
                ...item,
                trackereModels: models,
                deviceBindRule: item.deviceBindRule,
                deviceRechargeRules: item.deviceRechargeRules
            }
        })
    }

    // 根据用户资产id，反向查询设备及商户资产
    async getDeviceDataByAssetId(
        userId: string,
        assetId: string
    ): Promise<{
        tracker: Tracker | null
        trackerModel: TrackerModel | null
        asset: Asset | null
        user: User | null
    }> {
        const userAsset = await this.prisma.asset.findFirst({
            where: {
                id: assetId,
                userId: userId
            },
            include: {
                tracker: {
                    include: {
                        trackerModel: true
                    }
                },
                user: true
            }
        })

        if (userAsset == null) {
            return {
                tracker: null,
                trackerModel: null,
                asset: null,
                user: null
            }
        }

        return {
            tracker: userAsset.tracker ?? null,
            trackerModel: userAsset.tracker?.trackerModel ?? null,
            asset: userAsset,
            user: userAsset.user ?? null
        }
    }

    // 根据订单号查询订单，资产等数据
    async getOrderAndDeviceDataByOrderNumber(orderNumber: string): Promise<{
        order: AssetServicePeriodOrder | null
        tracker: Tracker | null
        userAsset: Asset | null
    }> {
        // 查询订单
        const order = await this.prisma.assetServicePeriodOrder.findFirst({
            where: {
                orderNumber: orderNumber
            }
        })

        if (order == null) {
            return {
                order: null,
                userAsset: null,
                tracker: null
            }
        }

        // 查询资产
        const userAsset = await this.prisma.asset.findFirst({
            where: {
                id: order.assetId
            },
            include: {
                tracker: {
                    include: {
                        trackerModel: true
                    }
                }
            }
        })

        if (userAsset == null) {
            return {
                order: order,
                userAsset: null,
                tracker: null
            }
        }

        return {
            order: order,
            userAsset: userAsset,
            tracker: userAsset.tracker ?? null
        }
    }

    async handlePaySuccess(
        order: AssetServicePeriodOrder,
        amount: number,
        paidAt: Dayjs,
        tracker: Tracker,
        userAsset: Asset,
        transactionId: string,
        attach: string | null
    ): Promise<O.Option<string>> {
        // 如果订单已经存在处理时间，则说明该订单已经被处理过，直接返回
        if (order.processedAt != null) {
            return O.some(`order has been processed, we just ignore it, order id: ${order.id}`)
        }

        if (order.orderTarget == OrderTargetEnums.Activate) {
            return await this.handleActivateSuccess(order, amount, paidAt, tracker, userAsset, transactionId)
        }

        if (order.orderTarget == OrderTargetEnums.Renewal) {
            return await this.handleRenewSuccess(order, amount, paidAt, userAsset, transactionId, attach)
        }

        return O.some(`unknown order target: ${order.orderTarget}, order id: ${order.id}`)
    }

    // 根据退款订单号查询订单
    async getOrderByRefundOrderNumber(refundOrderNumber: string): Promise<AssetServicePeriodOrder | null> {
        // 查询订单
        const order = await this.prisma.assetServicePeriodOrder.findFirst({
            where: {
                refundOrderNumber: refundOrderNumber
            }
        })

        if (order == null) {
            return null
        }

        return order
    }

    // 处理退款成功
    async handleRefundSuccess(order: AssetServicePeriodOrder, amount: number, refundedAt: Dayjs): Promise<O.Option<string>> {
        // 如果订单已经存在退款时间，则说明该订单已经被处理过，直接返回
        if (order.refundedAt != null) {
            return O.some(`order has been refunded, we just ignore it, order id: ${order.id}`)
        }

        // 更新退款时间，退款金额
        await this.prisma.assetServicePeriodOrder.update({
            where: {
                id: order.id
            },
            data: {
                refundedAmount: amount,
                refundedAt: refundedAt.toDate()
            }
        })

        return O.none
    }

    mapOrderStatus(order: AssetServicePeriodOrder): string {
        if (order.paidAt == null) {
            return AssetOrderStatusEnums.Unpaid
        }

        if (order.paidAt != null && order.refundedAt == null) {
            return AssetOrderStatusEnums.Paid
        }

        if (order.refundApplyAt != null && order.refundedAt == null) {
            return AssetOrderStatusEnums.RefundPending
        }

        if (order.refundedAt != null) {
            return AssetOrderStatusEnums.Refunded
        }

        return ''
    }

    private async handleActivateSuccess(
        order: AssetServicePeriodOrder,
        amount: number,
        paidAt: Dayjs,
        tracker: Tracker,
        userAsset: Asset,
        transactionId: string
    ): Promise<O.Option<string>> {
        // 更新支付时间，支付金额
        // 更新外部订单号 将外部订单号更新为 微信的交易单号
        await this.prisma.assetServicePeriodOrder.update({
            where: {
                id: order.id
            },
            data: {
                paidAmount: amount,
                paidAt: paidAt.toDate(),
                externalOrderNumber: transactionId,
                processedAt: getNow().toDate()
            }
        })

        // 计算服务开始时间和结束时间
        const timeRange = this.calculateServiceTimeRangeForOpen(
            tracker,
            {
                duration: order.servicePeriod,
                timeUnit: order.servicePeriodTimeUnit
            },
            order.giftDuration == null || order.giftTimeUnit == null
                ? null
                : {
                    duration: order.giftDuration,
                    timeUnit: order.giftTimeUnit
                }
        )

        // 更新资产的服务开始时间和结束时间
        await this.prisma.asset.update({
            where: {
                id: userAsset.id
            },
            data: {
                serviceStartTime: timeRange.startTime.toDate(),
                serviceEndTime: timeRange.endTime.toDate()
            }
        })

        await this.cdc.sendToCDC({
            kind: 'device-changed',
            id: tracker.id
        })

        // 判断是否需要进行分账 创建分账记录 无论是否成功都不影响充值的主流程
        if (order.isNeedProfitSharing) {
            // 创建分账记录
            await this.profitSharingService.createProfitSharingRecord(order)
        }

        return O.none
    }

    private async handleRenewSuccess(
        order: AssetServicePeriodOrder,
        amount: number,
        paidAt: Dayjs,
        userAsset: Asset,
        transactionId: string,
        attach: string | null
    ): Promise<O.Option<string>> {
        // 更新支付时间，支付金额
        await this.prisma.assetServicePeriodOrder.update({
            where: {
                id: order.id
            },
            data: {
                paidAmount: amount,
                paidAt: paidAt.toDate(),
                externalOrderNumber: transactionId,
                processedAt: getNow().toDate()
            }
        })

        // 如果资产没有服务结束时间，则返回错误
        if (userAsset.serviceEndTime == null) {
            return O.some(`userAsset service end time is null, we can not renew it, userAsset id: ${userAsset.id}, order id: ${order.id}`)
        }

        // 根据attach里面是否存在优惠券Id，来更新优惠券使用信息
        if (isNullOrEmptyString(attach) === false) {
            const coupon = await this.prisma.coupon.findFirst({
                where: {
                    id: attach,
                    activity: ActivityEnums.DOUBLE_TWELVE,
                    usedAt: null
                }
            })

            if (coupon) {
                await this.prisma.coupon.update({
                    where: { id: coupon.id },
                    data: {
                        usedAt: getNow().toDate(),
                        orderId: order.id
                    }
                })
            }
        }
        // 计算续费后的服务结束时间
        const newServiceEndTime = calculateServiceEndTimeForRenew(
            userAsset.serviceEndTime,
            {
                duration: order.servicePeriod,
                timeUnit: order.servicePeriodTimeUnit
            },
            order.giftDuration == null || order.giftTimeUnit == null
                ? null
                : {
                    duration: order.giftDuration,
                    timeUnit: order.giftTimeUnit
                }
        )

        // 更新资产的服务结束时间
        await this.prisma.asset.update({
            where: {
                id: userAsset.id
            },
            data: {
                serviceEndTime: newServiceEndTime.toDate()
            }
        })

        if (userAsset.trackerId != null) {
            await this.cdc.sendToCDC({
                kind: 'device-changed',
                id: userAsset.trackerId
            })
        }

        // 判断是否需要进行分账 创建分账记录 无论是否成功都不影响充值的主流程
        if (order.isNeedProfitSharing) {
            // 创建分账记录
            await this.profitSharingService.createProfitSharingRecord(order)
        }

        return O.none
    }

    // 为开通，计算服务周期
    calculateServiceTimeRangeForOpen(
        tracker: Tracker & { silentPeriodEndTime?: Date | null },
        rule: { duration: number; timeUnit: string },
        gift: { duration: number; timeUnit: string } | null
    ) {
        // 沉默期结束时间
        const silentPeriodEndTime = parseDate(tracker.silentPeriodEndTime!)

        // 当前时间
        const now = getNow()

        const calculateEndTimeForGift = (startTime: Dayjs) => {
            const endTime = calculateEndTime(startTime, rule.duration, rule.timeUnit)
            return gift == null ? endTime : calculateEndTime(endTime, gift.duration, gift.timeUnit)
        }

        // 如果当前时间在沉默期内，则开通时间为当前时间，结束时间为当前时间加上周期
        if (now.isBefore(silentPeriodEndTime)) {
            return {
                startTime: now,
                endTime: calculateEndTimeForGift(now)
            }
        }

        // 如果当前时间在沉默期外，则开通时间为沉默期结束时间，结束时间为沉默期结束时间加上周期
        return {
            startTime: silentPeriodEndTime,
            endTime: calculateEndTimeForGift(silentPeriodEndTime)
        }
    }
}
