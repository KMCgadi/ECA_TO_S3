package com.s3.eca2.domain.settingsCode;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

@Service
public class SettingsCodeService {
    private final SettingsCodeRepository settingsCodeRepository;

    public SettingsCodeService(SettingsCodeRepository settingsCodeRepository) {
        this.settingsCodeRepository = settingsCodeRepository;
    }

    public SettingsCode find(long entityId) {
        return settingsCodeRepository.findById(entityId)
                .orElseThrow(() -> new EntityNotFoundException("SettingsCode is not found for id: " + entityId));
    }

    public Page<SettingsCode> findSettingsCodeByDate(Date start, Date end, Pageable pageable) {
        return settingsCodeRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }

    public Page<SettingsCode> findAllSettingsCodes(Pageable pageable) {
        return settingsCodeRepository.findAll(pageable);
    }
}
