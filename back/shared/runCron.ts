import { spawn } from 'cross-spawn';
import dayjs from 'dayjs';
import taskLimit from './pLimit';
import Logger from '../loaders/logger';
import { ICron } from '../protos/cron';
import { CrontabModel, CrontabStatus } from '../data/cron';
import { CrontabStatModel } from '../data/cronStats';
import { killTask } from '../config/util';

export function runCron(cmd: string, cron: ICron): Promise<number | void> {
  return taskLimit.runWithCronLimit(cron, () => {
    return new Promise(async (resolve: any) => {
      // Check if the cron is already running and stop it (only if multiple instances are not allowed)
      try {
        const existingCron = await CrontabModel.findOne({
          where: { id: Number(cron.id) },
        });

        // Default to single instance mode (0) for backward compatibility
        const allowSingleInstances =
          existingCron?.allow_multiple_instances === 0;

        if (
          allowSingleInstances &&
          existingCron &&
          existingCron.pid &&
          (existingCron.status === CrontabStatus.running ||
            existingCron.status === CrontabStatus.queued)
        ) {
          Logger.info(
            `[schedule][停止已运行任务] 任务ID: ${cron.id}, PID: ${existingCron.pid}`,
          );
          await killTask(existingCron.pid);
          // Update the status to idle after killing
          await CrontabModel.update(
            { status: CrontabStatus.idle, pid: undefined },
            { where: { id: Number(cron.id) } },
          );
        }
      } catch (error) {
        Logger.error(
          `[schedule][检查已运行任务失败] 任务ID: ${cron.id}, 错误: ${error}`,
        );
      }

      Logger.info(
        `[schedule][开始执行任务] 参数 ${JSON.stringify({
          ...cron,
          command: cmd,
        })}`,
      );
      const startTime = Date.now();
      const cp = spawn(cmd, { shell: '/bin/bash' });

      cp.stderr.on('data', (data) => {
        Logger.info(
          '[schedule][执行任务失败] 命令: %s, 错误信息: %j',
          cmd,
          data.toString(),
        );
      });
      cp.on('error', (err) => {
        Logger.error(
          '[schedule][创建任务失败] 命令: %s, 错误信息: %j',
          cmd,
          err,
        );
      });

      cp.on('exit', async (code) => {
        const elapsed = Date.now() - startTime;
        taskLimit.removeQueuedCron(cron.id);
        Logger.info(
          '[schedule][执行任务结束] 参数: %s, 退出码: %j',
          JSON.stringify({
            ...cron,
            command: cmd,
          }),
          code,
        );

        // 写入统计
        try {
          const today = dayjs().format('YYYY-MM-DD');
          const isSuccess = code === 0 ? 1 : 0;
          const isFail = code !== 0 ? 1 : 0;
          const refId = Number(cron.id);

          const existing = await CrontabStatModel.findOne({
            where: { ref_id: refId, date: today },
          });

          if (existing) {
            await CrontabStatModel.update(
              {
                run_count: (existing.run_count || 0) + 1,
                success_count: (existing.success_count || 0) + isSuccess,
                fail_count: (existing.fail_count || 0) + isFail,
                total_time: (existing.total_time || 0) + elapsed,
                max_time: Math.max(existing.max_time || 0, elapsed),
              },
              { where: { id: existing.id } },
            );
          } else {
            await CrontabStatModel.create({
              ref_id: refId,
              date: today,
              run_count: 1,
              success_count: isSuccess,
              fail_count: isFail,
              total_time: elapsed,
              max_time: elapsed,
            });
          }
        } catch (err) {
          Logger.error('[schedule][统计写入失败]', err);
        }

        resolve({ ...cron, command: cmd, pid: cp.pid, code });
      });
    });
  });
}
