package com.s3.eca2.domain.organizationType;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;


@Service
public class OrganizationTypeService {

    private final OrganizationRepository organizationRepository;

    public OrganizationTypeService(OrganizationRepository organizationRepository) {
        this.organizationRepository = organizationRepository;
    }
    public OrganizationType find(long organizationTypeEid) {
        return organizationRepository.findById(organizationTypeEid)
                .orElseThrow(() -> new EntityNotFoundException("OrganizationType is not found for id: " + organizationTypeEid));
    }
    public Page<OrganizationType> findOrganizationTypeByDate(Date start, Date end, Pageable pageable) {
        return organizationRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
