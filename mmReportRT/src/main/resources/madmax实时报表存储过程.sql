BEGIN
	DECLARE request_time TIMESTAMP;
	DECLARE tracker_time TIMESTAMP;
	DECLARE t_error INTEGER DEFAULT 0;
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET t_error = 1;

	START TRANSACTION;
	SET @request_time = (
		SELECT MAX(create_time)
		FROM mm_report_project_request_mem
	);
	SET @tracker_time = (
		SELECT MAX(create_time)
		FROM mm_report_project_tracker_mem
	);
	SELECT sleep(1), @request_time, @tracker_time, NOW();

	IF @request_time IS NULL
	AND @tracker_time IS NULL THEN
		SELECT "目前没有已经处理完的数据" AS msg;
	ELSE 
		#project  <---  request
		INSERT INTO `mm_report_project_rt` (`timestamp`, `project_id`, `bids`, `create_time`)
		SELECT `timestamp`, `project_id`, SUM(bids) AS `bids`, min(`create_time`) as `create_time`
		FROM mm_report_project_request_mem
		WHERE create_time <= @request_time and project_id > 0
		GROUP BY `timestamp`, `project_id`
		ON DUPLICATE KEY UPDATE mm_report_project_rt.bids = mm_report_project_rt.bids + VALUES(bids)
		, `update_time` = VALUES(`create_time`);
		#project <---  track
		INSERT INTO `mm_report_project_rt` (`timestamp`, `project_id`, `wins`, `imps`
			, `clks`, `vimps`, `vclks`, `cost`, `create_time`)
		SELECT `timestamp`, `project_id`, SUM(wins) AS `wins`
			, SUM(imps) AS `imps`, SUM(clks) AS `clks`
			, SUM(vimps) AS `vimps`, SUM(vclks) AS `vclks`
			, SUM(cost) AS `cost`, min(`create_time`) as `create_time`
		FROM mm_report_project_tracker_mem
		WHERE create_time <= @tracker_time and project_id > 0
		GROUP BY `timestamp`, `project_id`
		ON DUPLICATE KEY UPDATE mm_report_project_rt.wins = mm_report_project_rt.wins + VALUES(wins)
		, mm_report_project_rt.imps = mm_report_project_rt.imps + VALUES(imps)
		, mm_report_project_rt.clks = mm_report_project_rt.clks + VALUES(clks)
		, mm_report_project_rt.vimps = mm_report_project_rt.vimps + VALUES(vimps)
		, mm_report_project_rt.vclks = mm_report_project_rt.vclks + VALUES(vclks)
		, mm_report_project_rt.cost = mm_report_project_rt.cost + VALUES(cost)
		, `update_time` = VALUES(`create_time`);
		
		#campaign <--- request
		INSERT INTO `mm_report_project_campaign_rt` (`timestamp`, `project_id`, `campaign_id`, `bids`
			, `create_time`)
		SELECT `timestamp`, `project_id`, `campaign_id`
			, SUM(bids) AS `bids`, min(`create_time`) as `create_time`
		FROM mm_report_project_request_mem
		WHERE create_time <= @request_time and project_id > 0
		GROUP BY `timestamp`, `project_id`, `campaign_id`
		ON DUPLICATE KEY UPDATE mm_report_project_campaign_rt.bids = mm_report_project_campaign_rt.bids + VALUES(bids)
		, `update_time` = VALUES(`create_time`);
		#campaign <--- track
		INSERT INTO `mm_report_project_campaign_rt` (`timestamp`, `project_id`, `campaign_id`,
		`wins`, `imps`, `clks`, `vimps`, `vclks`, `cost`, `create_time`)
		SELECT `timestamp`, `project_id`, `campaign_id`, SUM(wins) AS `wins`
			, SUM(imps) AS `imps`, SUM(clks) AS `clks`
			, SUM(vimps) AS `vimps`, SUM(vclks) AS `vclks`
			, SUM(cost) AS `cost`, min(`create_time`) as `create_time`
		FROM mm_report_project_tracker_mem
		WHERE create_time <= @tracker_time and project_id > 0
		GROUP BY `timestamp`, `project_id`, `campaign_id`
		ON DUPLICATE KEY UPDATE mm_report_project_campaign_rt.wins = mm_report_project_campaign_rt.wins + VALUES(wins)
		, mm_report_project_campaign_rt.imps = mm_report_project_campaign_rt.imps + VALUES(imps)
		, mm_report_project_campaign_rt.clks = mm_report_project_campaign_rt.clks + VALUES(clks)
		, mm_report_project_campaign_rt.vimps = mm_report_project_campaign_rt.vimps + VALUES(vimps)
		, mm_report_project_campaign_rt.vclks = mm_report_project_campaign_rt.vclks + VALUES(vclks)
		, mm_report_project_campaign_rt.cost = mm_report_project_campaign_rt.cost + VALUES(cost)
		, `update_time` = VALUES(`create_time`);
		
		
		#material <--- request
		INSERT INTO `mm_report_project_campaign_material_rt` (`timestamp`, `project_id`, `campaign_id`
		, `material_id`, `bids`, `create_time`)
		SELECT `timestamp`, `project_id`, `campaign_id`, `material_id`
			, SUM(bids) AS `bids`, min(`create_time`) as `create_time`
		FROM mm_report_project_request_mem
		WHERE create_time <= @request_time and project_id > 0
		GROUP BY `timestamp`, `project_id`, `campaign_id`, `material_id`
		ON DUPLICATE KEY UPDATE mm_report_project_campaign_material_rt.bids = mm_report_project_campaign_material_rt.bids + VALUES(bids)
		, `update_time` = VALUES(`create_time`);
		#material <--- track
		INSERT INTO `mm_report_project_campaign_material_rt` (`timestamp`, `project_id`, `campaign_id`
		, `material_id`, `wins`, `imps`, `clks`, `vimps`, `vclks`, `cost`, `create_time`)
		SELECT `timestamp`, `project_id`, `campaign_id`, `material_id`, SUM(wins) AS `wins`
			, SUM(imps) AS `imps`, SUM(clks) AS `clks`
			, SUM(vimps) AS `vimps`, SUM(vclks) AS `vclks`
			, SUM(cost) AS `cost`, min(`create_time`) as `create_time`
		FROM mm_report_project_tracker_mem
		WHERE create_time <= @tracker_time and project_id > 0
		GROUP BY `timestamp`, `project_id`, `campaign_id`, `material_id`
		ON DUPLICATE KEY UPDATE mm_report_project_campaign_material_rt.wins = mm_report_project_campaign_material_rt.wins + VALUES(wins)
		, mm_report_project_campaign_material_rt.imps = mm_report_project_campaign_material_rt.imps + VALUES(imps)
		, mm_report_project_campaign_material_rt.clks = mm_report_project_campaign_material_rt.clks + VALUES(clks)
		, mm_report_project_campaign_material_rt.vimps = mm_report_project_campaign_material_rt.vimps + VALUES(vimps)
		, mm_report_project_campaign_material_rt.vclks = mm_report_project_campaign_material_rt.vclks + VALUES(vclks)
		, mm_report_project_campaign_material_rt.cost = mm_report_project_campaign_material_rt.cost + VALUES(cost)
		, `update_time` = VALUES(`create_time`);
		
		
		#ssp <--- request
		INSERT INTO `mm_report_project_ssp_rt` (`timestamp`, `project_id`, `ssp_id`, `media_id`, `adspace_id`
		, `reqs`, `errs`, `bids`, `create_time`)
		SELECT `timestamp`, `project_id`, `ssp_id`, `media_id`, `adspace_id`, SUM(reqs) AS `reqs`
			, SUM(errs) AS `errs`, SUM(bids) AS `bids`
			, min(`create_time`) as `create_time`
		FROM mm_report_project_request_mem
		WHERE create_time <= @request_time and media_id > 0
		GROUP BY `timestamp`, `project_id`, `ssp_id`, `media_id`, `adspace_id`
		ON DUPLICATE KEY UPDATE mm_report_project_ssp_rt.reqs = mm_report_project_ssp_rt.reqs + VALUES(reqs)
		, mm_report_project_ssp_rt.errs = mm_report_project_ssp_rt.errs + VALUES(errs)
		, mm_report_project_ssp_rt.bids = mm_report_project_ssp_rt.bids + VALUES(bids)
		, `update_time` = VALUES(`create_time`);
		#ssp <--- track
		INSERT INTO `mm_report_project_ssp_rt` (`timestamp`, `project_id`, `ssp_id`, `media_id`, `adspace_id`
		, `wins`, `imps`, `clks`, `vimps`, `vclks`, `income`, `create_time`)
		SELECT `timestamp`, `project_id`, `ssp_id`, `media_id`, `adspace_id`, SUM(wins) AS `wins` 
			, SUM(imps) AS `imps`, SUM(clks) AS `clks`
			, SUM(vimps) AS `vimps`, SUM(vclks) AS `vclks`
			, SUM(income) AS `income`, min(`create_time`) as `create_time`
		FROM mm_report_project_tracker_mem
		WHERE create_time <= @tracker_time and media_id > 0
		GROUP BY `timestamp`, `project_id`, `ssp_id`, `media_id`, `adspace_id`
		ON DUPLICATE KEY UPDATE mm_report_project_ssp_rt.wins = mm_report_project_ssp_rt.wins + VALUES(wins)
		, mm_report_project_ssp_rt.imps = mm_report_project_ssp_rt.imps + VALUES(imps)
		, mm_report_project_ssp_rt.clks = mm_report_project_ssp_rt.clks + VALUES(clks)
		, mm_report_project_ssp_rt.vimps = mm_report_project_ssp_rt.vimps + VALUES(vimps)
		, mm_report_project_ssp_rt.vclks = mm_report_project_ssp_rt.vclks + VALUES(vclks)
		, mm_report_project_ssp_rt.income = mm_report_project_ssp_rt.income + VALUES(income)
		, `update_time` = VALUES(`create_time`);
		
		#delete the records have been processed
		DELETE FROM mm_report_project_request_mem WHERE create_time <= @request_time;
		DELETE FROM mm_report_project_tracker_mem WHERE create_time <= @tracker_time;
	END IF;
	IF t_error = 1 THEN
		ROLLBACK;
	ELSE 
		COMMIT;
	END IF;
	SELECT t_error, NOW();

END